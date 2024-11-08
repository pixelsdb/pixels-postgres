//
// Created by liyu on 3/26/23.
//


#include "PixelsReader.h"
#include "reader/PixelsRecordReader.h"

struct PixelsReadLocalState {
    PixelsReadLocalState() {
        curr_file_index = 0;
        next_file_index = 0;
        curr_batch_index = 0;
        next_batch_index = 0;
        rowOffset = 0;
        currPixelsRecordReader = nullptr;
        nextPixelsRecordReader = nullptr;
        vectorizedRowBatch = nullptr;
        currReader = nullptr;
        nextReader = nullptr;
    }
	std::shared_ptr<PixelsRecordReader> currPixelsRecordReader;
    std::shared_ptr<PixelsRecordReader> nextPixelsRecordReader;
	std::shared_ptr<VectorizedRowBatch> vectorizedRowBatch;
    int deviceID;
	int rowOffset;
	std::vector<uint64_t> column_ids;
	std::vector<std::string> column_names;
	std::shared_ptr<PixelsReader> currReader;
    std::shared_ptr<PixelsReader> nextReader;
	uint64_t curr_file_index;
    uint64_t next_file_index;
    uint64_t curr_batch_index;
    uint64_t next_batch_index;
    std::string next_file_name;
    std::string curr_file_name;

};
