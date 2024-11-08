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

#define MAX_PIXELS_OPTION_LENGTH 100

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

extern "C" void
pixelsGetForeignRelSize(PlannerInfo *root,
                        RelOptInfo *baserel,
                        Oid foreigntableid)
{
    PixelsFdwPlanState *fdw_private;
    char* filename = (char*)pixelsGetOption(foreigntableid,
                     						"filename");
	List* options = pixelsGetOptions(foreigntableid);
	fdw_private = createPixelsFdwPlanState(filename,
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

	/* Create the ForeignScan node */
	return make_foreignscan(tlist,
							scan_clauses,
							scan_relid,
							NIL,	/* no expressions to evaluate */
							best_path->fdw_private,
							NIL,	/* no custom tlist */
							NIL,	/* no remote quals */
							outer_plan);
}

extern "C" void
pixelsExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
    PixelsFdwPlanState *fdw_private;
    char* filename = (char*)pixelsGetOption(RelationGetRelid(node->ss.ss_currentRelation),
                     						"filename");
	List* options = pixelsGetOptions(RelationGetRelid(node->ss.ss_currentRelation));
	fdw_private = createPixelsFdwPlanState(filename, options);
	ExplainPropertyText("Pixels File Name: ",
						 filename,
                         es);
}

extern "C" void
pixelsBeginForeignScan(ForeignScanState *node, int eflags)
{
	char* filename = (char*)pixelsGetOption(RelationGetRelid(node->ss.ss_currentRelation),
                     						"filename");

	PixelsFdwExecutionState *festate;
	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	festate = createPixelsFdwExecutionState(filename, node->ss.ss_ScanTupleSlot->tts_tupleDescriptor);
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
    // TODO
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