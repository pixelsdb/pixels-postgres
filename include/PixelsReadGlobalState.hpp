//
// Created by liyu on 3/26/23.
//

#include "PixelsReader.h"
#include "physical/StorageArrayScheduler.h"

struct PixelsReadGlobalState {
	std::mutex lock;

	//! The initial reader from the bind phase
	std::shared_ptr<PixelsReader> initialPixelsReader;

	//! Mutexes to wait for a file that is currently being opened
	std::unique_ptr<std::mutex[]> file_mutexes;

	//! Signal to other threads that a file failed to open, letting every thread abort.
	bool error_opening_file = false;

    std::shared_ptr<StorageArrayScheduler> storageArrayScheduler;

	//! Index of file currently up for scanning
	std::vector<uint64_t> file_index;

	//! Batch index of the next row group to be scanned
	uint64_t batch_index;

	uint64_t max_threads;

	uint64_t MaxThreads() const {
		return max_threads;
	}
};
