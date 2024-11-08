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

#include "physical/storage/LocalFS.h"
#include "physical/natives/ByteBuffer.h"
#include "physical/natives/DirectRandomAccessFile.h"
#include "physical/io/PhysicalLocalReader.h"
#include "physical/StorageFactory.h"
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
	PixelsFdwExecutionState(string filename,
							TupleDesc tupleDesc);
	static unique_ptr<PixelsReadGlobalState> PixelsScanInitGlobal(PixelsReadBindData &bind_data);
	static unique_ptr<PixelsReadLocalState> PixelsScanInitLocal(PixelsReadBindData &bind_data,
															    PixelsReadGlobalState &parallel_state);
	static unique_ptr<PixelsReadBindData> PixelsScanBind(string filename,
														 vector<Oid> &return_types);
	static bool PixelsParallelStateNext(const PixelsReadBindData &bind_data,
	                                    PixelsReadLocalState &scan_data,
										PixelsReadGlobalState &parallel_state,
                                        bool is_init_state = false);
    static PixelsReaderOption GetPixelsReaderOption(PixelsReadLocalState &local_state,
													PixelsReadGlobalState &global_state);
	static void TransformPostgresType(const std::shared_ptr<TypeDescription> &type,
									  vector<Oid> &return_types);
	bool next(TupleTableSlot* slot);
	void rescan();
private:
	uint64_t cur_row_index = 0;
	unique_ptr<PixelsReadBindData> bind_data;
	unique_ptr<PixelsReadLocalState> scan_data; 
	unique_ptr<PixelsReadGlobalState> parallel_state;
	PixelsReaderOption reader_option;
	int current_location = 0;
};


PixelsFdwExecutionState* createPixelsFdwExecutionState(string filename,
													   TupleDesc tupleDesc);