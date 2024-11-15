//
// Created by liyu on 3/26/23.
//

#include "PixelsFdwExecutionState.hpp"

char *
tolowercase(const char *input, char *output)
{
    int i = 0;
    Assert(strlen(input) < NAMEDATALEN - 1);
    do
    {
        output[i] = tolower(input[i]);
    }
    while (input[i++]);

    return output;
}



PixelsFdwExecutionState::PixelsFdwExecutionState(List* files,
												 List* filters,
											     set<int> attrs_used,
												 TupleDesc tupleDesc) {
	ListCell *file_lc;
	foreach (file_lc, files) {
		files_list.emplace_back(std::string(strVal(lfirst(file_lc))));
	}
	ListCell *filter_lc;
	foreach (filter_lc, filters) {
		filters_list.emplace_back((PixelsFilter*)lfirst(filter_lc));
	}
	attrs_used = attrs_used;
	tuple_desc = tupleDesc;
	shared_ptr<TypeDescription> file_schema;
	bind_data = PixelsFdwExecutionState::PixelsScanBind(files_list, filters_list, file_schema);
	column_map = PixelsFdwExecutionState::PixelsGetColumnMap(file_schema, attrs_used, tuple_desc);
	parallel_state = PixelsFdwExecutionState::PixelsScanInitGlobal(*bind_data);
	scan_data = PixelsFdwExecutionState::PixelsScanInitLocal(*bind_data, *parallel_state, column_map);
}

PixelsFdwExecutionState::~PixelsFdwExecutionState() {
	if (bind_data->initialPixelsReader) {
		bind_data->initialPixelsReader->close();
	}
	bind_data.reset();
	if (parallel_state->initialPixelsReader) {
		parallel_state->initialPixelsReader->close();
	}
	parallel_state.reset();
	if (scan_data->currReader) {
		scan_data->currReader->close();
	}
	if (scan_data->currPixelsRecordReader) {
		scan_data->currPixelsRecordReader->close();
	}
	if (scan_data->nextPixelsRecordReader) {
		scan_data->nextPixelsRecordReader->close();
	}
	scan_data.reset();
}


static uint32_t PixelsScanGetBatchIndex(const PixelsReadBindData *bind_data_p,
                                        PixelsReadLocalState *local_state,
                                        PixelsReadGlobalState *global_state) {
	auto &data = (PixelsReadLocalState &)*local_state;
	return data.curr_batch_index;
}

static double PixelsProgress(const PixelsReadBindData *bind_data_p,
                             const PixelsReadGlobalState *global_state) {
	auto &bind_data = (PixelsReadBindData &)*bind_data_p;
	if (bind_data.files.empty()) {
		return 100.0;
	}
	auto percentage = bind_data.curFileId * 100.0 / bind_data.files.size();
	return percentage;
}

static uint32_t PixelsCardinality(const PixelsReadBindData *bind_data) {
	auto &data = (PixelsReadBindData &)*bind_data;

	return data.initialPixelsReader->getNumberOfRows() * data.files.size();
}

unique_ptr<PixelsReadBindData>
PixelsFdwExecutionState::PixelsScanBind(vector<string> filenames,
										vector<PixelsFilter*> filters_list,
                           				shared_ptr<TypeDescription> &file_schema) {
	if (filenames.empty()) {
		throw PixelsReaderException("Pixels reader cannot take empty filename as parameter");
	}

	auto footerCache = std::make_shared<PixelsFooterCache>();
	auto builder = std::make_shared<PixelsReaderBuilder>();

	std::shared_ptr<::Storage> storage = StorageFactory::getInstance()->getStorage(::Storage::file);
	std::shared_ptr<PixelsReader> pixelsReader = builder->setPath(filenames.at(0))
	                                 					->setStorage(storage)
	                                 					->setPixelsFooterCache(footerCache)
	                                 					->build();
	file_schema = pixelsReader->getFileSchema();

	auto result = make_unique<PixelsReadBindData>();
	result->initialPixelsReader = pixelsReader;
	result->fileSchema = file_schema;
	result->files = filenames;
	result->filters = filters_list;

	return std::move(result);
}

vector<int>
PixelsFdwExecutionState::PixelsGetColumnMap(const shared_ptr<TypeDescription> file_schema,
										    set<int> attrs_used,
											TupleDesc tupleDesc) {
	vector<int> column_map;
	column_map.resize(tupleDesc->natts);
    for (int i = 0; i < tupleDesc->natts; i++)
    {
        AttrNumber  attnum = i + 1 - FirstLowInvalidHeapAttributeNumber;
        char        pg_colname[255];
        const char *attname = NameStr(TupleDescAttr(tupleDesc, i)->attname);

        column_map[i] = -1;

        /* Skip columns we don't intend to use in query */
        if (attrs_used.find(attnum) == attrs_used.end())
            continue;

        tolowercase(NameStr(TupleDescAttr(tupleDesc, i)->attname), pg_colname);
		assert(file_schema->getCategory() == TypeDescription::STRUCT);
		vector<char*> column_names;
        for (int i = 0; i < file_schema->getChildren().size(); i++)
        {
            auto field_name = file_schema->getFieldNames().at(i);
            char pixels_colname[255];
            if (field_name.length() > NAMEDATALEN)
                throw PixelsReaderException("pixels column name is too long");
            tolowercase(file_schema->getFieldNames().at(i).c_str(), pixels_colname);
            if (strcmp(pg_colname, pixels_colname) == 0)
            {
                column_names.push_back(pixels_colname);
                column_map[i] = column_names.size() - 1;
                break;
            }
        }
    }
	return column_map;
}

unique_ptr<PixelsReadGlobalState>
PixelsFdwExecutionState::PixelsScanInitGlobal(PixelsReadBindData &bind_data) {

	auto result = make_unique<PixelsReadGlobalState>();
	result->initialPixelsReader = bind_data.initialPixelsReader;
    int max_threads = std::stoi(ConfigFactory::Instance().getProperty("pixel.threads"));
    if (max_threads <= 0) {
        max_threads = (int) bind_data.files.size();
    }
    result->storageArrayScheduler = std::make_shared<StorageArrayScheduler>(bind_data.files, max_threads);
    result->file_index.resize(result->storageArrayScheduler->getDeviceSum());
	result->max_threads = max_threads;
	result->batch_index = 0;
	return std::move(result);
}

unique_ptr<PixelsReadLocalState>
PixelsFdwExecutionState::PixelsScanInitLocal(PixelsReadBindData &bind_data,
											 PixelsReadGlobalState &parallel_state,
											 vector<int> column_map) {
	auto result = make_unique<PixelsReadLocalState>();
    result->deviceID = parallel_state.storageArrayScheduler->acquireDeviceId();
	auto file_schema = bind_data.fileSchema;
	vector<string> field_names;
	vector<uint64_t> field_ids;
	for (int i = 0; i < column_map.size(); i++) {
		if (column_map[i] >= 0) {
			field_names.emplace_back(file_schema->getFieldNames().at(i));
			field_ids.emplace_back(i);
		}
	}
	result->filters = bind_data.filters;
	result->column_names = field_names;
	result->column_ids = field_ids;
	if(!PixelsParallelStateNext(bind_data, *result, parallel_state, true)) {
		return nullptr;
	}
	return std::move(result);
}

bool
PixelsFdwExecutionState::PixelsParallelStateNext(const PixelsReadBindData &bind_data,
                                                 PixelsReadLocalState &scan_data,
                                                 PixelsReadGlobalState &parallel_state,
                                                 bool is_init_state) {
    unique_lock<mutex> parallel_lock(parallel_state.lock);
    if (parallel_state.error_opening_file) {
        throw InvalidArgumentException("PixelsScanInitLocal: file open error.");
    }

    auto& StorageInstance = parallel_state.storageArrayScheduler;
    if ((is_init_state &&
		 parallel_state.file_index.at(scan_data.deviceID) >= StorageInstance->getFileSum(scan_data.deviceID)) ||
         scan_data.next_file_index >= StorageInstance->getFileSum(scan_data.deviceID)) {
		::BufferPool::Reset();
        parallel_lock.unlock();
        return false;
    }

    scan_data.curr_file_index = scan_data.next_file_index;
    scan_data.curr_batch_index = scan_data.next_batch_index;
    scan_data.next_file_index = parallel_state.file_index.at(scan_data.deviceID);
    scan_data.next_batch_index = StorageInstance->getBatchID(scan_data.deviceID, scan_data.next_file_index);
    scan_data.curr_file_name = scan_data.next_file_name;
    parallel_state.file_index.at(scan_data.deviceID)++;
    parallel_lock.unlock();    

    if(scan_data.currReader != nullptr) {
        scan_data.currReader->close();
    }

    ::BufferPool::Switch();
    scan_data.currReader = scan_data.nextReader;
    scan_data.currPixelsRecordReader = scan_data.nextPixelsRecordReader;
	if (scan_data.currPixelsRecordReader != nullptr) {
        auto currPixelsRecordReader = std::static_pointer_cast<PixelsRecordReaderImpl>(scan_data.currPixelsRecordReader);
        currPixelsRecordReader->read();
    }
    if(scan_data.next_file_index < StorageInstance->getFileSum(scan_data.deviceID)) {
        auto footerCache = std::make_shared<PixelsFooterCache>();
        auto builder = std::make_shared<PixelsReaderBuilder>();
        std::shared_ptr<::Storage> storage = StorageFactory::getInstance()->getStorage(::Storage::file);
        scan_data.next_file_name = StorageInstance->getFileName(scan_data.deviceID, scan_data.next_file_index);
        scan_data.nextReader = builder->setPath(scan_data.next_file_name)
                					  ->setStorage(storage)
                					  ->setPixelsFooterCache(footerCache)
                					  ->build();

        PixelsReaderOption option = GetPixelsReaderOption(scan_data, parallel_state);
        scan_data.nextPixelsRecordReader = scan_data.nextReader->read(option);
        auto nextPixelsRecordReader = std::static_pointer_cast<PixelsRecordReaderImpl>(scan_data.nextPixelsRecordReader);
        nextPixelsRecordReader->read();
    } else {
        scan_data.nextReader = nullptr;
        scan_data.nextPixelsRecordReader = nullptr;
    }
    return true;
}

PixelsReaderOption
PixelsFdwExecutionState::GetPixelsReaderOption(PixelsReadLocalState &local_state,
											   PixelsReadGlobalState &global_state) {
    PixelsReaderOption option;
    option.setSkipCorruptRecords(true);
    option.setTolerantSchemaEvolution(true);
    option.setEnableEncodedColumnVector(true);
    option.setIncludeCols(local_state.column_names);
	option.setRGRange(0, local_state.nextReader->getRowGroupNum());
    option.setQueryId(1);
	option.setEnabledFilterPushDown(true);
	option.setFilters(local_state.filters);
    int stride = std::stoi(ConfigFactory::Instance().getProperty("pixel.stride"));
    option.setBatchSize(stride);
    return option;
}

bool PixelsFdwExecutionState::next(TupleTableSlot* slot) {
	if (!scan_data) {
		return false;
	}
	if (scan_data->currPixelsRecordReader == nullptr) {
        if(!PixelsParallelStateNext(*bind_data, *scan_data, *parallel_state)) {
            return false;
        }
    }
	if (scan_data->currPixelsRecordReader->isEndOfFile() && scan_data->vectorizedRowBatch->isEndOfFile()) {
		scan_data->currPixelsRecordReader.reset();
        if(!PixelsParallelStateNext(*bind_data, *scan_data, *parallel_state)) {
            return false;
        }
	}
    auto currPixelsRecordReader = static_pointer_cast<PixelsRecordReaderImpl>(scan_data->currPixelsRecordReader);
	if (scan_data->vectorizedRowBatch != nullptr && scan_data->vectorizedRowBatch->isEndOfFile()) {
        scan_data->vectorizedRowBatch = nullptr;
    }
	if (scan_data->vectorizedRowBatch == nullptr) {
        scan_data->vectorizedRowBatch = currPixelsRecordReader->readBatch(false);
		cur_row_index = 0;
    }
    if (cur_row_index >= scan_data->vectorizedRowBatch->rowCount) {
		scan_data->vectorizedRowBatch = currPixelsRecordReader->readBatch(false);
		cur_row_index = 0;
	}
	for (int i = 0; i < scan_data->column_ids.size(); i++) {
		int column_id = scan_data->column_ids.at(i);
		auto col = scan_data->vectorizedRowBatch->cols.at(i);
		auto colSchema = bind_data->fileSchema->getChildren().at(column_id);
		switch (colSchema->getCategory()) {
			case TypeDescription::SHORT: {
			    auto intCol = std::static_pointer_cast<LongColumnVector>(col);
                slot->tts_isnull[column_id] = false;
				slot->tts_values[column_id] = Int16GetDatum(*((short*)(intCol->current())));
			    break;
			}
			case TypeDescription::INT: {
				auto intCol = std::static_pointer_cast<LongColumnVector>(col);
                slot->tts_isnull[column_id] = false;
				slot->tts_values[column_id] = Int32GetDatum(*((int*)(intCol->current())));
			    break;
		    }
			case TypeDescription::LONG: {
				auto intCol = std::static_pointer_cast<LongColumnVector>(col);
                slot->tts_isnull[column_id] = false;
				slot->tts_values[column_id] = Int64GetDatum(*((long*)(intCol->current())));
			    break;
			}
			case TypeDescription::DATE: {
				auto intCol = std::static_pointer_cast<DateColumnVector>(col);
                slot->tts_isnull[column_id] = false;
				slot->tts_values[column_id] = DateADTGetDatum(*((int*)(intCol->current())) + (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE));
			    break;
		    }
			case TypeDescription::DECIMAL: {
				auto decimalCol = std::static_pointer_cast<DecimalColumnVector>(col);
				if (decimalCol->getPrecision() > 18) {
					throw PixelsReaderException("Pixels reader do not support longer decimal");
				}
				// lose precision
				Datum numeric_data = DirectFunctionCall1(float8_numeric,
													 	 Float8GetDatum(float8(*((long*)(decimalCol->current())) / std::pow(10, decimalCol->getScale()))));
                slot->tts_isnull[column_id] = false;
				slot->tts_values[column_id] = numeric_data;
			    break;
		    }
			case TypeDescription::VARCHAR:
			case TypeDescription::CHAR:
		    {
			    auto binaryCol = std::static_pointer_cast<BinaryColumnVector>(col);
	            string_t *string_value = (string_t*)(binaryCol->current());
            	int64 bytea_len = string_value->GetSize() + VARHDRSZ;
            	bytea *b = (bytea*)palloc0(bytea_len);
            	SET_VARSIZE(b, bytea_len);
            	memcpy(VARDATA(b), string_value->GetData(), string_value->GetSize());
                slot->tts_isnull[column_id] = false;
				slot->tts_values[column_id] = PointerGetDatum(b);
			    break;
		    }
            default: {
				throw PixelsReaderException("Pixels reader do not support other types now");
				break;
			}	
		}       
    }
    cur_row_index++;
	scan_data->vectorizedRowBatch->increment(1);
	ExecStoreVirtualTuple(slot);
    return true;
}

void
PixelsFdwExecutionState::PixelsFdwExecutionState::rescan() {
	bind_data->initialPixelsReader->close();
	bind_data.reset();
	parallel_state->initialPixelsReader->close();
	parallel_state.reset();
	scan_data->currReader->close();
	scan_data->currPixelsRecordReader->close();
	scan_data->nextPixelsRecordReader->close();
	scan_data.reset();
	shared_ptr<TypeDescription> file_schema;
	bind_data = PixelsFdwExecutionState::PixelsScanBind(files_list, filters_list, file_schema);
	column_map = PixelsFdwExecutionState::PixelsGetColumnMap(file_schema, attrs_used, tuple_desc);
	parallel_state = PixelsFdwExecutionState::PixelsScanInitGlobal(*bind_data);
	scan_data = PixelsFdwExecutionState::PixelsScanInitLocal(*bind_data, *parallel_state, column_map);

}

PixelsFdwExecutionState*
createPixelsFdwExecutionState(List* filenames,
							  List* filters,
							  set<int> attrs_used,
							  TupleDesc tupleDesc) {
    return new PixelsFdwExecutionState(filenames, filters, attrs_used, tupleDesc);
}
