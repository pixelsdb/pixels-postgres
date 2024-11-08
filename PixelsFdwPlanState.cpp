//
// Created by liyu on 3/26/23.
//

#include "PixelsFdwPlanState.hpp"
#include "physical/StorageArrayScheduler.h"
#include "profiler/CountProfiler.h"

PixelsFdwPlanState::PixelsFdwPlanState(string filename,
									   List* options) {
    
	rel_data = PixelsFdwPlanState::PixelsBindMetaData(filename);
    row_count = rel_data->initialPixelsReader->getNumberOfRows();
    plan_options = options;
}

unique_ptr<PixelsRelMetaData>
PixelsFdwPlanState::PixelsBindMetaData(string filename) {
	if (filename.empty()) {
		throw PixelsReaderException("Pixels reader cannot take empty filename as parameter");
	}

	auto footerCache = std::make_shared<PixelsFooterCache>();
	auto builder = std::make_shared<PixelsReaderBuilder>();

	std::shared_ptr<::Storage> storage = StorageFactory::getInstance()->getStorage(::Storage::file);
	std::shared_ptr<PixelsReader> pixelsReader = builder
	                                 ->setPath(filename)
	                                 ->setStorage(storage)
	                                 ->setPixelsFooterCache(footerCache)
	                                 ->build();
	std::shared_ptr<TypeDescription> fileSchema = pixelsReader->getFileSchema();

	auto result = make_unique<PixelsRelMetaData>();
	result->initialPixelsReader = pixelsReader;
	vector<string> filenames;
	filenames.emplace_back(filename);
	result->files = filenames;

	return std::move(result);
}

uint64_t
PixelsFdwPlanState::getRowCount() {
    return row_count;
}

PixelsFdwPlanState*
createPixelsFdwPlanState(string filename,
							   List* options) {
    return new PixelsFdwPlanState(filename, options);
}
