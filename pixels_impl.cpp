#include "PixelsFdwExecutionState.hpp"
#include "PixelsFdwPlanState.hpp"

extern "C"
{
#include "postgres.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "executor/spi.h"
#include "executor/tuptable.h"
#include "foreign/foreign.h"
#include "foreign/fdwapi.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "parser/parse_coerce.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/regproc.h"
#include "utils/rel.h"
#include "utils/typcache.h"
#include "access/table.h"
#include "optimizer/optimizer.h"
#include "catalog/pg_am_d.h"
#include "nodes/pathnodes.h"
}

#define MAX_PIXELS_OPTION_LENGTH 500

static void*
pixelsGetOption(Oid relid, char* option_name)
{
	if (!option_name) {
		elog(ERROR,
             "empty option name");
		return nullptr;
	}
    ForeignTable *table;
	ForeignServer *server;
	ForeignDataWrapper *wrapper;
	List	   *options;
	ListCell   *lc;

	table = GetForeignTable(relid);
	server = GetForeignServer(table->serverid);
	wrapper = GetForeignDataWrapper(server->fdwid);

	options = NIL;
	options = list_concat(options, wrapper->options);
	options = list_concat(options, server->options);
	options = list_concat(options, table->options);

    foreach(lc, table->options)
    {
		DefElem    *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, option_name) == 0)
        {
            void* result_option = (char *) palloc0(MAX_PIXELS_OPTION_LENGTH);
            memcpy(result_option,
                   defGetString(def),
				   MAX_PIXELS_OPTION_LENGTH);
			return result_option;
        }
    }
	elog(ERROR,
         "unknown option '%s'",
         option_name);
	return nullptr;
}

static List*
pixelsGetOptions(Oid relid)
{
    ForeignTable *table;
	ForeignServer *server;
	ForeignDataWrapper *wrapper;
	List	   *options;
	ListCell   *lc;

	table = GetForeignTable(relid);
	server = GetForeignServer(table->serverid);
	wrapper = GetForeignDataWrapper(server->fdwid);

	options = NIL;
	options = list_concat(options, wrapper->options);
	options = list_concat(options, server->options);
	options = list_concat(options, table->options);

	return options;
}

typedef enum
{
    FPS_START = 0,
    FPS_IDENT,
    FPS_QUOTE
} FileParserState;

static void
parse_filenames_list(const char *str, List* &filenames)
{
    char       					*cur = pstrdup(str);
    char       					*f = cur;
    FileParserState 			state = FPS_START;
    while (*cur)
    {
        switch (state)
        {
            case FPS_START:
                switch (*cur)
                {
                    case ' ':
                        /* just skip */
                        break;
                    case '|':
                        f = cur + 1;
                        state = FPS_QUOTE;
                        break;
                    default:
                        /* XXX we should check that *cur is a valid path symbol
                         * but let's skip it for now */
                        state = FPS_IDENT;
                        f = cur;
                        break;
                }
                break;
            case FPS_IDENT:
                switch (*cur)
                {
                    case ' ':
                        *cur = '\0';
                        filenames = lappend(filenames, makeString(f));
                        state = FPS_START;
                        break;
                    default:
                        break;
                }
                break;
            case FPS_QUOTE:
                switch (*cur)
                {
                    case '|':
                        *cur = '\0';
                        filenames = lappend(filenames, makeString(f));
                        state = FPS_START;
                        break;
                    default:
                        break;
                }
                break;
            default:
                elog(ERROR, "pixels_fdw: unknown filename parse state");
        }
        cur++;
    }
}

typedef enum
{
    FT_DIGIT = 0,
    FT_DECIMAL,
    FT_AND,
    FT_OR,
    FT_GTEQ,
    FT_LTEQ,
    FT_EQ,
    FT_GT,
    FT_LT,
    FT_LB,
    FT_RB,
    FT_WORD,
    FT_MISMATCH
} FilterType;


static void
parse_filter_type(const char *str,
                  PixelsFilter* &all_filters,
                  List* &refered_cols)
{
    std::string s = std::string(str);
    assert(!s.empty());
    std::regex delim("\\s+");
    std::regex_token_iterator<std::string::iterator> split(s.begin(), s.end(), delim, -1);
    std::regex_token_iterator<std::string::iterator> rend;
    
    std::vector<regex> regexs;
    regexs.emplace_back(std::regex("\\d+"));
    regexs.emplace_back(std::regex("\\d*\\.\\d*"));
    regexs.emplace_back(std::regex("&"));
    regexs.emplace_back(std::regex("\\|"));
    regexs.emplace_back(std::regex(">="));
    regexs.emplace_back(std::regex("<="));
    regexs.emplace_back(std::regex("=="));
    regexs.emplace_back(std::regex(">"));
    regexs.emplace_back(std::regex("<"));
    regexs.emplace_back(std::regex("\\("));
    regexs.emplace_back(std::regex("\\)"));
    regexs.emplace_back(std::regex("\\w+"));

    std::vector<FilterType> optypes;
    std::vector<std::string> opnames;
    
    while (split != rend) {
        std::string sub = *split++;
        FilterType t = FT_DIGIT;
        for (; t < FT_MISMATCH; t = (FilterType)(t + 1)) {
            if (std::regex_match(sub, regexs.at(t))) {
                break;
            }
        }
        if (t == FT_MISMATCH) {
            ereport(ERROR,
                (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                 errmsg("pixels_fdw: invalid filter option \"%s\"",
                        sub.c_str())));
        }
        optypes.emplace_back(t);
        opnames.emplace_back(sub);
    }

    optypes.emplace_back(FT_MISMATCH);  // Invalid

    std::stack<FilterType> optypes_stack;
    std::stack<std::pair<std::string, bool>> opnames_stack;

    int ipriority[] = {-1, -1, 4, 2, 6, 6, 6, 6, 6, 0, 7, -1, -1};
    int opriority[] = {-1, -1, 3, 1, 5, 5, 5, 5, 5, 7, 0, -1, -1};

    std::stack<PixelsFilter*> filters;

    optypes_stack.push(FT_MISMATCH); // Invalid
    for (int i = 0; i < optypes.size(); ) {
        if (optypes.at(i) == FT_DIGIT || optypes.at(i) == FT_DECIMAL) {
            opnames_stack.push(std::make_pair(opnames.front(), true));
            opnames.erase(opnames.begin());
            i++;
        }
        else if (optypes.at(i) == FT_WORD) {
            char *refered_col = (char*)palloc0(opnames.front().length() + 1);
            strcpy(refered_col, opnames.front().c_str());
            refered_cols = lappend(refered_cols, makeString(refered_col));
            opnames_stack.push(std::make_pair(opnames.front(), false));
            opnames.erase(opnames.begin());
            i++;
        }
        else {
            if (opriority[optypes.at(i)] > ipriority[optypes_stack.top()]) {
                optypes_stack.push(optypes.at(i));
                i++;
            }
            else if (opriority[optypes.at(i)] < ipriority[optypes_stack.top()]) {
                switch (optypes_stack.top()) {
                    case FT_AND: {
                        PixelsFilter *and_filter = createPixelsFilter(PixelsFilterType::CONJUNCTION_AND, std::string(), 0, 0, string_t());
                        if (filters.size() < 2) {
                            ereport(ERROR,
                                errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                                errmsg("pixels_fdw: invalid filter option, parse error "));
                        }
                        PixelsFilter *filter_2 = filters.top();
                        filters.pop();
                        PixelsFilter *filter_1 = filters.top();
                        filters.pop();
                        and_filter->setLChild(filter_1);
                        and_filter->setRChild(filter_2);
                        filters.push(and_filter);
                        break;
                    }
                    case FT_OR: {
                        PixelsFilter *or_filter = createPixelsFilter(PixelsFilterType::CONJUNCTION_OR, std::string(), 0, 0, string_t());
                        if (filters.size() < 2) {
                            ereport(ERROR,
                                errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                                errmsg("pixels_fdw: invalid filter option, parse error "));
                        }
                        PixelsFilter *filter_2 = filters.top();
                        filters.pop();
                        PixelsFilter *filter_1 = filters.top();
                        filters.pop();
                        or_filter->setLChild(filter_1);
                        or_filter->setRChild(filter_2);
                        filters.push(or_filter);
                        break;
                    }
                    case FT_GTEQ: {
                        if (opnames_stack.size() < 2) {
                            ereport(ERROR,
                                errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                                errmsg("pixels_fdw: invalid filter option, parse error "));
                        }
                        std::pair<std::string, bool> oprand_2 = opnames_stack.top();
                        opnames_stack.pop();
                        std::pair<std::string, bool> oprand_1 = opnames_stack.top();
                        opnames_stack.pop();
                        if (oprand_1.second == oprand_2.second) {
                            ereport(ERROR,
                                errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                                errmsg("pixels_fdw: invalid filter option, parse error "));
                        }
                        if (!oprand_1.second) {
                            long ivalue;
                            double dvalue;
                            sscanf(oprand_2.first.c_str(), "%ld", &ivalue);
                            sscanf(oprand_2.first.c_str(), "%lf", &dvalue);
                            string_t svalue = string_t(oprand_2.first.c_str());
                            char *cname = (char*)palloc0(oprand_1.first.size());
                            strcpy(cname, oprand_1.first.c_str());

                            PixelsFilter *gteq_filter = createPixelsFilter(PixelsFilterType::COMPARE_GTEQ, oprand_1.first, ivalue, dvalue, svalue);
                            filters.push(gteq_filter);
                        }
                        else {
                            long ivalue;
                            double dvalue;
                            sscanf(oprand_1.first.c_str(), "%ld", &ivalue);
                            sscanf(oprand_1.first.c_str(), "%lf", &dvalue);
                            string_t svalue = string_t(oprand_1.first.c_str());
                            char *cname = (char*)palloc0(oprand_2.first.size());
                            strcpy(cname, oprand_2.first.c_str());

                            PixelsFilter *gteq_filter = createPixelsFilter(PixelsFilterType::COMPARE_LTEQ, oprand_2.first, ivalue, dvalue, svalue);
                            filters.push(gteq_filter);
                        }
                        break;
                    }
                    case FT_LTEQ: {
                        if (opnames_stack.size() < 2) {
                            ereport(ERROR,
                                errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                                errmsg("pixels_fdw: invalid filter option, parse error "));
                        }
                        std::pair<std::string, bool> oprand_2 = opnames_stack.top();
                        opnames_stack.pop();
                        std::pair<std::string, bool> oprand_1 = opnames_stack.top();
                        opnames_stack.pop();
                        if (oprand_1.second == oprand_2.second) {
                            ereport(ERROR,
                                errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                                errmsg("pixels_fdw: invalid filter option, parse error "));
                        }
                        if (!oprand_1.second) {
                            long ivalue;
                            double dvalue;
                            sscanf(oprand_2.first.c_str(), "%ld", &ivalue);
                            sscanf(oprand_2.first.c_str(), "%lf", &dvalue);
                            string_t svalue = string_t(oprand_2.first.c_str());
                            char *cname = (char*)palloc0(oprand_1.first.size());
                            strcpy(cname, oprand_1.first.c_str());

                            PixelsFilter *gteq_filter = createPixelsFilter(PixelsFilterType::COMPARE_LTEQ, oprand_1.first, ivalue, dvalue, svalue);
                            filters.push(gteq_filter);
                        }
                        else {
                            long ivalue;
                            double dvalue;
                            sscanf(oprand_1.first.c_str(), "%ld", &ivalue);
                            sscanf(oprand_1.first.c_str(), "%lf", &dvalue);
                            string_t svalue = string_t(oprand_1.first.c_str());
                            char *cname = (char*)palloc0(oprand_2.first.size());
                            strcpy(cname, oprand_2.first.c_str());

                            PixelsFilter *gteq_filter = createPixelsFilter(PixelsFilterType::COMPARE_GTEQ, oprand_2.first, ivalue, dvalue, svalue);
                            filters.push(gteq_filter);
                        }
                        break;
                    }
                    case FT_EQ: {
                        if (opnames_stack.size() < 2) {
                            ereport(ERROR,
                                errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                                errmsg("pixels_fdw: invalid filter option, parse error "));
                        }
                        std::pair<std::string, bool> oprand_2 = opnames_stack.top();
                        opnames_stack.pop();
                        std::pair<std::string, bool> oprand_1 = opnames_stack.top();
                        opnames_stack.pop();
                        if (oprand_1.second == oprand_2.second) {
                            ereport(ERROR,
                                errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                                errmsg("pixels_fdw: invalid filter option, parse error "));
                        }
                        if (!oprand_1.second) {
                            long ivalue;
                            double dvalue;
                            sscanf(oprand_2.first.c_str(), "%ld", &ivalue);
                            sscanf(oprand_2.first.c_str(), "%lf", &dvalue);
                            string_t svalue = string_t(oprand_2.first.c_str());
                            char *cname = (char*)palloc0(oprand_1.first.size());
                            strcpy(cname, oprand_1.first.c_str());

                            PixelsFilter *gteq_filter = createPixelsFilter(PixelsFilterType::COMPARE_LTEQ, oprand_1.first, ivalue, dvalue, svalue);
                            filters.push(gteq_filter);
                        }
                        else {
                            long ivalue;
                            double dvalue;
                            sscanf(oprand_1.first.c_str(), "%ld", &ivalue);
                            sscanf(oprand_1.first.c_str(), "%lf", &dvalue);
                            string_t svalue = string_t(oprand_1.first.c_str());
                            char *cname = (char*)palloc0(oprand_2.first.size());
                            strcpy(cname, oprand_2.first.c_str());

                            PixelsFilter *gteq_filter = createPixelsFilter(PixelsFilterType::COMPARE_GTEQ, oprand_2.first, ivalue, dvalue, svalue);
                            filters.push(gteq_filter);
                        }
                        break;
                    }
                    case FT_GT: {
                        if (opnames_stack.size() < 2) {
                            ereport(ERROR,
                                errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                                errmsg("pixels_fdw: invalid filter option, parse error "));
                        }
                        std::pair<std::string, bool> oprand_2 = opnames_stack.top();
                        opnames_stack.pop();
                        std::pair<std::string, bool> oprand_1 = opnames_stack.top();
                        opnames_stack.pop();
                        if (oprand_1.second == oprand_2.second) {
                            ereport(ERROR,
                                errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                                errmsg("pixels_fdw: invalid filter option, parse error "));
                        }
                        if (!oprand_1.second) {
                            long ivalue;
                            double dvalue;
                            sscanf(oprand_2.first.c_str(), "%ld", &ivalue);
                            sscanf(oprand_2.first.c_str(), "%lf", &dvalue);
                            string_t svalue = string_t(oprand_2.first.c_str());
                            char *cname = (char*)palloc0(oprand_1.first.size());
                            strcpy(cname, oprand_1.first.c_str());

                            PixelsFilter *gteq_filter = createPixelsFilter(PixelsFilterType::COMPARE_GT, oprand_1.first, ivalue, dvalue, svalue);
                            filters.push(gteq_filter);
                        }
                        else {
                            long ivalue;
                            double dvalue;
                            sscanf(oprand_1.first.c_str(), "%ld", &ivalue);
                            sscanf(oprand_1.first.c_str(), "%lf", &dvalue);
                            string_t svalue = string_t(oprand_1.first.c_str());
                            char *cname = (char*)palloc0(oprand_2.first.size());
                            strcpy(cname, oprand_2.first.c_str());

                            PixelsFilter *gteq_filter = createPixelsFilter(PixelsFilterType::COMPARE_LT, oprand_2.first, ivalue, dvalue, svalue);
                            filters.push(gteq_filter);
                        }
                        break;
                    }
                    case FT_LT: {
                        if (opnames_stack.size() < 2) {
                            ereport(ERROR,
                                errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                                errmsg("pixels_fdw: invalid filter option, parse error "));
                        }
                        std::pair<std::string, bool> oprand_2 = opnames_stack.top();
                        opnames_stack.pop();
                        std::pair<std::string, bool> oprand_1 = opnames_stack.top();
                        opnames_stack.pop();
                        if (oprand_1.second == oprand_2.second) {
                            ereport(ERROR,
                                errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                                errmsg("pixels_fdw: invalid filter option, parse error "));
                        }
                        if (!oprand_1.second) {
                            long ivalue;
                            double dvalue;
                            sscanf(oprand_2.first.c_str(), "%ld", &ivalue);
                            sscanf(oprand_2.first.c_str(), "%lf", &dvalue);
                            string_t svalue = string_t(oprand_2.first.c_str());
                            char *cname = (char*)palloc0(oprand_1.first.size());
                            strcpy(cname, oprand_1.first.c_str());

                            PixelsFilter *gteq_filter = createPixelsFilter(PixelsFilterType::COMPARE_LT, oprand_1.first, ivalue, dvalue, svalue);
                            filters.push(gteq_filter);
                        }
                        else {
                            long ivalue;
                            double dvalue;
                            sscanf(oprand_1.first.c_str(), "%ld", &ivalue);
                            sscanf(oprand_1.first.c_str(), "%lf", &dvalue);
                            string_t svalue = string_t(oprand_1.first.c_str());
                            char *cname = (char*)palloc0(oprand_2.first.size());
                            strcpy(cname, oprand_2.first.c_str());

                            PixelsFilter *gteq_filter = createPixelsFilter(PixelsFilterType::COMPARE_GT, oprand_2.first, ivalue, dvalue, svalue);
                            filters.push(gteq_filter);
                        }
                        break;
                    }
                    default: {
                        ereport(ERROR,
                            errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                            errmsg("pixels_fdw: invalid filter option, parse error "));
                    }                     
                }
                optypes_stack.pop();
            }
            else {
                optypes_stack.pop();
                i++;
            }
            if (!opnames.empty()) {
                opnames.erase(opnames.begin());
            }
        }
    }
    if (opnames_stack.size() != 0 || filters.size() != 1) {
        ereport(ERROR,
                errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                errmsg("pixels_fdw: invalid filter option, parse error"));
    }
    all_filters = filters.top();
}

static void
search_and_merge(const char *col_name, PixelsFilter *root, PixelsFilter *&new_filter) {
    if (!root->getColumnName().empty()) {
        if ((root->getColumnName().compare(std::string(col_name)) == 0)) {
            new_filter = root->copy();
            return;
        }
        return;
    }
    else {
        assert(root->getFilterType() == PixelsFilterType::CONJUNCTION_AND || root->getFilterType() == PixelsFilterType::CONJUNCTION_OR);
        assert(root->getLChild());
        assert(root->getRChild());
        PixelsFilter *new_lchild = nullptr;
        search_and_merge(col_name, root->getLChild(), new_lchild);
        PixelsFilter *new_rchild = nullptr;
        search_and_merge(col_name, root->getRChild(), new_rchild);
        if (!new_lchild && !new_rchild) {
            return;
        }
        else if (new_lchild && !new_rchild) {
            new_filter = new_lchild;
            return;
        }
        else if (!new_lchild && new_rchild) {
            new_filter = new_rchild;
            return;
        }
        else {
            new_filter = root->copy();
            new_filter->setColumnName(std::string(col_name));
            new_filter->setLChild(new_lchild);
            new_filter->setRChild(new_rchild);
            return;
        }
    }
}

static void
separate_filters(PixelsFilter* &all_filters,
                 List* &refered_cols, 
                 List* &col_filters) {
    ListCell *lc;
	foreach (lc, refered_cols) {
		char *col_name = strVal(lfirst(lc));
        PixelsFilter* col_filter = nullptr;
        search_and_merge(col_name, all_filters, col_filter);
        if(col_filter) {
            col_filters = lappend(col_filters, col_filter);
        }
	}
}

extern "C" void
pixelsGetForeignRelSize(PlannerInfo *root,
                        RelOptInfo *baserel,
                        Oid foreigntableid) {
    PixelsFdwPlanState *fdw_private;
    char* filename = (char*)pixelsGetOption(foreigntableid,
                     						"filename");
	List* filenames = NIL;
	parse_filenames_list(filename, filenames);
    char* filters = (char*)pixelsGetOption(foreigntableid,
                     					   "filters");
    PixelsFilter* all_filters;
	List* refered_cols = NIL;
	parse_filter_type(filters, all_filters, refered_cols);
    List* col_filters = NIL;
    separate_filters(all_filters, refered_cols, col_filters);
    List* options = pixelsGetOptions(foreigntableid);
	fdw_private = createPixelsFdwPlanState(filenames,
                                           col_filters,
										   options);
    baserel->fdw_private = fdw_private;
    baserel->tuples = fdw_private->getRowCount();
	baserel->rows = fdw_private->getRowCount();
}

static void
estimate_costs(PlannerInfo *root,
               RelOptInfo *baserel,
			   PixelsFdwPlanState *fdw_private,
               Cost *startup_cost,
			   Cost *run_cost,
               Cost *total_cost)
{
    double  			ntuples;

    ntuples = baserel->tuples * clauselist_selectivity(root,
                                					   baserel->baserestrictinfo,
                                					   0,
                                					   JOIN_INNER,
                                					   NULL);

    /*
     * Here we assume that parquet tuple cost is the same as regular tuple cost
     * even though this is probably not true in many cases. Maybe we'll come up
     * with a smarter idea later. Also we use actual number of rows in selected
     * rowgroups to calculate cost as we need to process those rows regardless
     * of whether they're gonna be filtered out or not.
     */
    *run_cost = fdw_private->getRowCount() * cpu_tuple_cost;
	*startup_cost = baserel->baserestrictcost.startup;
	*total_cost = *startup_cost + *run_cost;

    baserel->rows = ntuples;
}

static void
extract_used_attributes(RelOptInfo *baserel)
{
    PixelsFdwPlanState *fdw_private = (PixelsFdwPlanState *) baserel->fdw_private;
    ListCell *lc;

    pull_varattnos((Node *) baserel->reltarget->exprs,
                   baserel->relid,
                   &fdw_private->attrs_used);

    foreach(lc, baserel->baserestrictinfo)
    {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

        pull_varattnos((Node *) rinfo->clause,
                       baserel->relid,
                       &fdw_private->attrs_used);
    }

    if (bms_is_empty(fdw_private->attrs_used))
    {
        bms_free(fdw_private->attrs_used);
        fdw_private->attrs_used = bms_make_singleton(1 - FirstLowInvalidHeapAttributeNumber);
    }
}

extern "C" void
pixelsGetForeignPaths(PlannerInfo *root,
					  RelOptInfo *baserel,
					  Oid foreigntableid)
{
	PixelsFdwPlanState *fdw_private = (PixelsFdwPlanState *) baserel->fdw_private;
	Cost		startup_cost;
	Cost        run_cost;
	Cost		total_cost;

	/* Estimate costs */
	estimate_costs(root, baserel,
                   fdw_private,
                   &startup_cost,
				   &run_cost,
                   &total_cost);
    
    extract_used_attributes(baserel);

	add_path(baserel, 
             (Path *)
			 create_foreignscan_path(root,
                                     baserel,
									 NULL,	/* default pathtarget */
									 baserel->rows,
									 startup_cost,
									 total_cost,
									 NIL,	/* no pathkeys */
									 baserel->lateral_relids,
									 NULL,	/* no extra plan */
									 NIL));
	/* assume there is no parallel paths, so run_cost omitted*/
}

extern "C" ForeignScan *
pixelsGetForeignPlan(PlannerInfo *root,
				     RelOptInfo *baserel,
				     Oid foreigntableid,
				     ForeignPath *best_path,
				     List *tlist,
				     List *scan_clauses,
				     Plan *outer_plan)
{
	PixelsFdwPlanState *fdw_private = (PixelsFdwPlanState *) baserel->fdw_private;
	List       *params = NIL;
    List       *attrs_used = NIL;
	AttrNumber  attr;
	Index		scan_relid = baserel->relid;

	/*
	 * We have no native ability to evaluate restriction clauses, so we just
	 * put all the scan_clauses into the plan node's qual list for the
	 * executor to check.  So all we have to do here is strip RestrictInfo
	 * nodes from the clauses and ignore pseudoconstants (which will be
	 * handled elsewhere).
	 */
	scan_clauses = extract_actual_clauses(scan_clauses,
                                          false);
							
	attr = -1;
    while ((attr = bms_next_member(fdw_private->attrs_used, attr)) >= 0)
        attrs_used = lappend_int(attrs_used, attr);

	params = lappend(params, fdw_private->getFilesList());
	params = lappend(params, fdw_private->getFiltersList());
    params = lappend(params, attrs_used);

	/* Create the ForeignScan node */
	return make_foreignscan(tlist,
							scan_clauses,
							scan_relid,
							NIL,	/* no expressions to evaluate */
							params,
							NIL,	/* no custom tlist */
							NIL,	/* no remote quals */
							outer_plan);
}

extern "C" void
pixelsExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	char* filename = (char*)pixelsGetOption(RelationGetRelid(node->ss.ss_currentRelation),
                     						"filename");
	ExplainPropertyText("Pixels File Names: ",
						 filename,
                         es);
	char* filters = (char*)pixelsGetOption(RelationGetRelid(node->ss.ss_currentRelation),
                     						"filters");
	ExplainPropertyText("Pixels Table Filters: ",
						 filters,
                         es);
}

extern "C" void
pixelsBeginForeignScan(ForeignScanState *node, int eflags)
{
	List      		*fdw_private = ((ForeignScan *)(node->ss.ps.plan))->fdw_private;
	ListCell		*lc, *lc2;
	List        	*filenames = NIL;
	List        	*filters = NIL;
	List            *attrs_list;
    std::set<int>   attrs_used;
	int             i = 0;
	foreach (lc, fdw_private)
    {
        switch(i)
        {
            case 0:
                filenames = (List *) lfirst(lc);
                break;
            case 1:
                filters = (List *) lfirst(lc);
                break;
            case 2:
                attrs_list = (List *) lfirst(lc);
                foreach (lc2, attrs_list)
                    attrs_used.insert(lfirst_int(lc2));
                break;
        }
        ++i;
    }
	PixelsFdwExecutionState *festate;
	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;
	festate = createPixelsFdwExecutionState(filenames,
                                            filters,
                                            attrs_used,
                                            node->ss.ss_ScanTupleSlot->tts_tupleDescriptor);
	node->fdw_state = (void *) festate;
}


/*
 * pixelsIterateForeignScan
 *		Read next record from the data file and store it into the
 *		ScanTupleSlot as a virtual tuple
 */
extern "C" TupleTableSlot *
pixelsIterateForeignScan(ForeignScanState *node)
{
	PixelsFdwExecutionState *festate = (PixelsFdwExecutionState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	ExecClearTuple(slot);
	if (festate->next(slot)) {
		return slot;
	}
	return NULL;
}

extern "C" void
pixelsReScanForeignScan(ForeignScanState *node)
{
   	PixelsFdwExecutionState *festate = (PixelsFdwExecutionState *) node->fdw_state;
	festate->rescan();
}

extern "C" void
pixelsEndForeignScan(ForeignScanState *node)
{
    PixelsFdwExecutionState *festate = (PixelsFdwExecutionState *) node->fdw_state;
	delete festate;
}

extern "C" bool
pixelsAnalyzeForeignTable(Relation relation,
						  AcquireSampleRowsFunc *func,
						  BlockNumber *totalpages) {
	return false;
}

extern "C" bool
pixelsIsForeignScanParallelSafe(PlannerInfo *root, RelOptInfo *rel,
										  RangeTblEntry *rte) {
	return false;
}

extern "C" Datum 
pixels_fdw_validator_impl(PG_FUNCTION_ARGS) {
	List       *options = untransformRelOptions(PG_GETARG_DATUM(0));
    Oid         catalog = PG_GETARG_OID(1);
    ListCell   *opt_lc;
    bool        filename_provided = false;
    bool        filters_provided = false;

    /* Only check table options */
    if (catalog != ForeignTableRelationId)
        PG_RETURN_VOID();

    foreach(opt_lc, options)
    {
        DefElem    *def = (DefElem *) lfirst(opt_lc);

        if (strcmp(def->defname, "filename") == 0)
        {
            char   *filename = pstrdup(defGetString(def));
            if (filename) {
				filename_provided = true;
			}
        }
        else if (strcmp(def->defname, "filters") == 0)
        {
            char   *filters = pstrdup(defGetString(def));
            if (filters) {
				filters_provided = true;
			}
        }
        else
        {
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                     errmsg("pixels_fdw: invalid option \"%s\"",
                            def->defname)));
        }
    }

    if (!filename_provided)
        elog(ERROR, "pixels_fdw: filename is required");

    PG_RETURN_VOID();
}