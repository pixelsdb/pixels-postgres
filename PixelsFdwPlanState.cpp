//
// Created by liyu on 3/26/23.
//

#include "PixelsFdwPlanState.hpp"
#include "physical/StorageArrayScheduler.h"
#include "profiler/CountProfiler.h"

PixelsFdwPlanState::PixelsFdwPlanState(List* files,
									   List* col_filters,
									   List* options) {
	ListCell *file_lc;
	foreach (file_lc, files) {
		files_list = lappend(files_list, lfirst(file_lc));
	}
	if (list_length(files_list) == 0) {
		throw PixelsReaderException("Pixels reader cannot take empty filename as parameter");
	}

	ListCell *filter_lc;
	foreach (filter_lc, col_filters) {
		filters_list = lappend(filters_list, lfirst(filter_lc));
	}

	auto footerCache = std::make_shared<PixelsFooterCache>();
	auto builder = std::make_shared<PixelsReaderBuilder>();

	std::shared_ptr<::Storage> storage = StorageFactory::getInstance()->getStorage(::Storage::file);
	std::shared_ptr<PixelsReader> pixelsReader = builder->setPath(string(strVal(lfirst(list_head(files_list)))))
	                                 					->setStorage(storage)
	                                 					->setPixelsFooterCache(footerCache)
	                                 					->build();
	initialPixelsReader = pixelsReader;
    row_count = initialPixelsReader->getNumberOfRows();
    plan_options = options;
	attrs_used = bms_make_singleton(1 - FirstLowInvalidHeapAttributeNumber);
}

PixelsFdwPlanState::~PixelsFdwPlanState() {
	if (initialPixelsReader) {
		initialPixelsReader->close();
	}
	initialPixelsReader.reset();
}

List*&
PixelsFdwPlanState::getFilesList() {
    return files_list;
}

List*&
PixelsFdwPlanState::getFiltersList() {
    return filters_list;
}

uint64_t
PixelsFdwPlanState::getRowCount() {
    return row_count;
}


PixelsFdwPlanState*
createPixelsFdwPlanState(List* files,
						 List* col_filters,
						 List* options) {
    return new PixelsFdwPlanState(files, col_filters, options);
}
