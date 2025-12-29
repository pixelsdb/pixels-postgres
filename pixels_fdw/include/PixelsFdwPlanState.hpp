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

#include "physical/storage/LocalFS.h"
#include "physical/natives/ByteBuffer.h"
#include "physical/natives/DirectRandomAccessFile.h"
#include "physical/io/PhysicalLocalReader.h"
#include "physical/StorageFactory.h"
#include "PixelsReaderImpl.h"
#include "PixelsReaderBuilder.h"
#include "PixelsFilter.hpp"
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
	PixelsFdwPlanState(List* files,
                       List* filters,
                       List* options);
    ~PixelsFdwPlanState();
	List*& getFilesList();
    List*& getFiltersList();
    uint64_t getRowCount();
    Bitmapset* attrs_used;

private:
	std::shared_ptr<PixelsReader> initialPixelsReader;
	List* files_list = NIL;
    List* filters_list = NIL;
    uint64_t row_count;
    List* plan_options;
};


PixelsFdwPlanState* createPixelsFdwPlanState(List* files,
                                             List* filters,
											 List* options);