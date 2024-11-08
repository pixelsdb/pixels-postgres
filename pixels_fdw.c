#include "postgres.h"
#include "fmgr.h"

#include "commands/explain.h"
#include "foreign/fdwapi.h"


PG_MODULE_MAGIC;

void _PG_init(void);

/* FDW routines */
extern void pixelsGetForeignRelSize(PlannerInfo *root,
                      RelOptInfo *baserel,
                      Oid foreigntableid);
extern void pixelsGetForeignPaths(PlannerInfo *root,
                    RelOptInfo *baserel,
                    Oid foreigntableid);
extern ForeignScan *pixelsGetForeignPlan(PlannerInfo *root,
                      RelOptInfo *baserel,
                      Oid foreigntableid,
                      ForeignPath *best_path,
                      List *tlist,
                      List *scan_clauses,
                      Plan *outer_plan);
extern TupleTableSlot *pixelsIterateForeignScan(ForeignScanState *node);
extern void pixelsBeginForeignScan(ForeignScanState *node, int eflags);
extern void pixelsEndForeignScan(ForeignScanState *node);
extern void pixelsReScanForeignScan(ForeignScanState *node);
extern bool pixelsAnalyzeForeignTable (Relation relation,
                            AcquireSampleRowsFunc *func,
                            BlockNumber *totalpages);
extern void pixelsExplainForeignScan(ForeignScanState *node, ExplainState *es);
extern bool pixelsIsForeignScanParallelSafe(PlannerInfo *root, RelOptInfo *rel,
                                             RangeTblEntry *rte);
extern Datum pixels_fdw_validator_impl(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pixels_fdw_validator);
Datum
pixels_fdw_validator(PG_FUNCTION_ARGS)
{
    return pixels_fdw_validator_impl(fcinfo);
}

PG_FUNCTION_INFO_V1(pixels_fdw_handler);
Datum
pixels_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine *fdwroutine = makeNode(FdwRoutine);

    fdwroutine->GetForeignRelSize = pixelsGetForeignRelSize;
    fdwroutine->GetForeignPaths = pixelsGetForeignPaths;
    fdwroutine->GetForeignPlan = pixelsGetForeignPlan;
    fdwroutine->BeginForeignScan = pixelsBeginForeignScan;
    fdwroutine->IterateForeignScan = pixelsIterateForeignScan;
    fdwroutine->ReScanForeignScan = pixelsReScanForeignScan;
    fdwroutine->EndForeignScan = pixelsEndForeignScan;
    fdwroutine->AnalyzeForeignTable = pixelsAnalyzeForeignTable;
    fdwroutine->ExplainForeignScan = pixelsExplainForeignScan;
    fdwroutine->IsForeignScanParallelSafe = pixelsIsForeignScanParallelSafe;

    PG_RETURN_POINTER(fdwroutine);
}

