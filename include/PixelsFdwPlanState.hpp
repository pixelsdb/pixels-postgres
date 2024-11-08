//
// Created by liyu on 3/26/23.
//
#pragma once

#define STANDARD_VECTOR_SIZE 2048U

#include <fstream>
#include <iostream>
#include <numeric>
#include <string>
#include <vector>
#include <cstdio>
#include "PixelsRelMetaData.hpp"

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
#include "catalog/pg_type.h"
#include "executor/tuptable.h"
#include "executor/spi.h"
}

using namespace std;


class PixelsFdwPlanState {
public:
	PixelsFdwPlanState(string filename, 
                       List* options);
	static unique_ptr<PixelsRelMetaData> PixelsBindMetaData(string filename);
    uint64_t getRowCount();
private:
	unique_ptr<PixelsRelMetaData> rel_data;
    uint64_t row_count;
    List* plan_options;
};


PixelsFdwPlanState* createPixelsFdwPlanState(string filename,
											 List* options);