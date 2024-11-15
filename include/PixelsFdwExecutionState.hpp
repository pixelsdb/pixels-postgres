//
// Created by liyu on 3/26/23.
//
#pragma once

#define STANDARD_VECTOR_SIZE 2048U
#define PIXELS_FDW_MAX_DEC_WIDTH 18

#include <fstream>
#include <iostream>
#include <numeric>
#include <string>
#include <vector>
#include <cstdio>
#include "PixelsReadGlobalState.hpp"
#include "PixelsReadLocalState.hpp"
#include "PixelsReadBindData.hpp"
#include "PixelsFilter.hpp"
#include "physical/storage/LocalFS.h"
#include "physical/natives/ByteBuffer.h"
#include "physical/natives/DirectRandomAccessFile.h"
#include "physical/io/PhysicalLocalReader.h"
#include "physical/StorageFactory.h"
#include "physical/StorageArrayScheduler.h"
#include "profiler/CountProfiler.h"
#include "PixelsReaderImpl.h"
#include "PixelsReaderBuilder.h"
#include <iostream>
#include <future>
#include <thread>
#include "physical/scheduler/NoopScheduler.h"
#include "physical/SchedulerFactory.h"
#include "PixelsVersion.h"
#include "PixelsFooterCache.h"
#include "exception/PixelsReaderException.h"
#include "reader/PixelsReaderOption.h"
#include "TypeDescription.h"
#include "vector/ColumnVector.h"
#include "vector/LongColumnVector.h"
#include "physical/BufferPool.h"
#include "profiler/TimeProfiler.h"
#include "TypeDescription.h"
#include <cmath>

extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "utils/date.h"
#include "utils/numeric.h"
#include "utils/fmgrprotos.h"
#include "catalog/pg_type.h"
#include "executor/tuptable.h"
#include "executor/spi.h"
}

using namespace std;


class PixelsFdwExecutionState {
public:
	PixelsFdwExecutionState(List* files,
							List* filters,
							set<int> attrs_used,
							TupleDesc tupleDesc);
	~PixelsFdwExecutionState();
	static unique_ptr<PixelsReadGlobalState> PixelsScanInitGlobal(PixelsReadBindData &bind_data);
	static unique_ptr<PixelsReadLocalState> PixelsScanInitLocal(PixelsReadBindData &bind_data,
															    PixelsReadGlobalState &parallel_state,
																vector<int> column_map);
	static unique_ptr<PixelsReadBindData> PixelsScanBind(vector<string> files,
														 vector<PixelsFilter*> filters,
														 shared_ptr<TypeDescription> &file_schema);
	static vector<int> PixelsGetColumnMap(const shared_ptr<TypeDescription> file_schema,
										  set<int> attrs_used,
										  TupleDesc tupleDesc);
	static bool PixelsParallelStateNext(const PixelsReadBindData &bind_data,
	                                    PixelsReadLocalState &scan_data,
										PixelsReadGlobalState &parallel_state,
                                        bool is_init_state = false);
    static PixelsReaderOption GetPixelsReaderOption(PixelsReadLocalState &local_state,
													PixelsReadGlobalState &global_state);
	bool next(TupleTableSlot* slot);
	void rescan();
private:
	vector<string> files_list;
	vector<PixelsFilter*> filters_list;
	set<int> attrs_used;
	vector<int> column_map;
	vector<Oid> types;
	TupleDesc tuple_desc;
	uint64_t cur_row_index = 0;
	unique_ptr<PixelsReadBindData> bind_data;
	unique_ptr<PixelsReadLocalState> scan_data; 
	unique_ptr<PixelsReadGlobalState> parallel_state;
	PixelsReaderOption reader_option;
	vector<string> selected_column_name;
	vector<string> selected_column_idx;
	bool enable_filter_pushdown = true;
};

PixelsFdwExecutionState* createPixelsFdwExecutionState(List* files,
													   List* filters,
													   set<int> attrs_used,
													   TupleDesc tupleDesc);