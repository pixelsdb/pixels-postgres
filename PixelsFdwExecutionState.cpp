//
// Created by liyu on 3/26/23.
//

#include "PixelsFdwExecutionState.hpp"
#include "physical/StorageArrayScheduler.h"
#include "profiler/CountProfiler.h"

PixelsFdwExecutionState::PixelsFdwExecutionState(string filename,
												 TupleDesc tupleDesc) {
	vector<Oid> return_types;
	for (int i = 0; i < tupleDesc->natts; i++) {
		return_types.push_back(TupleDescAttr(tupleDesc, i)->atttypid);
	}
	vector<uint64_t> column;
	bind_data = PixelsFdwExecutionState::PixelsScanBind(filename, return_types);
	parallel_state = PixelsFdwExecutionState::PixelsScanInitGlobal(*bind_data);
	scan_data = PixelsFdwExecutionState::PixelsScanInitLocal(*bind_data, *parallel_state);
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

void
PixelsFdwExecutionState::TransformPostgresType(const std::shared_ptr<TypeDescription>& type,
                      						   vector<Oid> &return_types)
{
    auto columnSchemas = type->getChildren();
	for(auto columnType: columnSchemas) {
		switch (columnType->getCategory()) {
			case TypeDescription::SHORT:
				return_types.emplace_back(INT2OID);
			    break;
			case TypeDescription::INT:
				return_types.emplace_back(INT4OID);
			    break;
			case TypeDescription::LONG:
				return_types.emplace_back(INT8OID);
			    break;
			case TypeDescription::DECIMAL:
			    return_types.emplace_back(NUMERICOID);
				break;
			case TypeDescription::DATE:
			    return_types.emplace_back(DATEOID);
				break;
            case TypeDescription::TIMESTAMP:
                return_types.emplace_back(TIMESTAMPOID);
                break;
			case TypeDescription::VARCHAR:
				return_types.emplace_back(VARCHAROID);
				break;
			case TypeDescription::CHAR:
				return_types.emplace_back(CHAROID);
				break;
			default:
				throw InvalidArgumentException("bad column type in TransformDuckdbType: " + std::to_string(type->getCategory()));
		}
	}
}

unique_ptr<PixelsReadBindData>
PixelsFdwExecutionState::PixelsScanBind(string filename,
                           				vector<Oid> &return_types) {
	if (filename.empty()) {
		throw PixelsReaderException("Pixels reader cannot take empty filename as parameter");
	}

	auto footerCache = std::make_shared<PixelsFooterCache>();
	auto builder = std::make_shared<PixelsReaderBuilder>();

	std::shared_ptr<::Storage> storage = StorageFactory::getInstance()->getStorage(::Storage::file);
	std::shared_ptr<PixelsReader> pixelsReader = builder->setPath(filename)
	                                 					->setStorage(storage)
	                                 					->setPixelsFooterCache(footerCache)
	                                 					->build();
	std::shared_ptr<TypeDescription> fileSchema = pixelsReader->getFileSchema();
	TransformPostgresType(fileSchema, return_types);

	auto result = make_unique<PixelsReadBindData>();
	result->initialPixelsReader = pixelsReader;
	result->fileSchema = fileSchema;
	vector<string> filenames;
	filenames.emplace_back(filename);
	result->files = filenames;

	return std::move(result);
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
											 PixelsReadGlobalState &parallel_state) {
	auto result = make_unique<PixelsReadLocalState>();
    result->deviceID = parallel_state.storageArrayScheduler->acquireDeviceId();
	auto field_names = bind_data.fileSchema->getFieldNames();
	result->column_names = field_names;
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
		current_location = 0;
    }
    if (current_location >= scan_data->vectorizedRowBatch->rowCount) {
		scan_data->vectorizedRowBatch = currPixelsRecordReader->readBatch(false);
		current_location = 0;
	}
    std::shared_ptr<TypeDescription> resultSchema = scan_data->currPixelsRecordReader->getResultSchema();
    for (int attr = 0; attr < slot->tts_tupleDescriptor->natts; attr++) {
		auto col = scan_data->vectorizedRowBatch->cols.at(attr);
		auto colSchema = bind_data->fileSchema->getChildren().at(attr);
		switch (colSchema->getCategory()) {
			case TypeDescription::SHORT: {
			    auto intCol = std::static_pointer_cast<LongColumnVector>(col);
                slot->tts_isnull[attr] = false;
				slot->tts_values[attr] = Int16GetDatum(*((short*)(intCol->current())));
			    break;
			}
			case TypeDescription::INT: {
				auto intCol = std::static_pointer_cast<LongColumnVector>(col);
                slot->tts_isnull[attr] = false;
				slot->tts_values[attr] = Int32GetDatum(*((int*)(intCol->current())));
			    break;
		    }
			case TypeDescription::LONG: {
				auto intCol = std::static_pointer_cast<LongColumnVector>(col);
                slot->tts_isnull[attr] = false;
				slot->tts_values[attr] = Int64GetDatum(*((long*)(intCol->current())));
			    break;
			}
			case TypeDescription::DATE: {
				auto intCol = std::static_pointer_cast<DateColumnVector>(col);
                slot->tts_isnull[attr] = false;
				slot->tts_values[attr] = DateADTGetDatum(*((int*)(intCol->current())) + (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE));
			    break;
		    }
			case TypeDescription::DECIMAL: {
				auto decimalCol = std::static_pointer_cast<DecimalColumnVector>(col);
				if (decimalCol->getPrecision() > 18) {
					throw PixelsReaderException("Pixels reader do not support longer decimal");

				}
				char *numeric_str = (char *)palloc0(PIXELS_FDW_MAX_DEC_WIDTH + 2);
				// lose precision
				sprintf(numeric_str, "%lf", *((long*)(decimalCol->current())) / decimalCol->getScale());
				Datum numeric_data = DirectFunctionCall3(numeric_in,
													 	 CStringGetDatum(numeric_str),
													 	 ObjectIdGetDatum(InvalidOid),
													 	 Int32GetDatum(-1));
                slot->tts_isnull[attr] = false;
				slot->tts_values[attr] = numeric_data;
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
                slot->tts_isnull[attr] = false;
				slot->tts_values[attr] = PointerGetDatum(b);
			    break;
		    }
            default: {
				throw PixelsReaderException("Pixels reader do not support other types now");
				break;
			}	
		}       
    }
    current_location++;
	scan_data->vectorizedRowBatch->increment(1);
	ExecStoreVirtualTuple(slot);
	for (int attr = 0; attr < slot->tts_tupleDescriptor->natts; attr++) {
		std::cout << slot->tts_values[attr] << std::endl;
	}
    return true;
}

void PixelsFdwExecutionState::PixelsFdwExecutionState::rescan() {
	bind_data->curFileId = 0;
	scan_data->curr_batch_index = 0;
	scan_data->curr_file_name = bind_data->files.front();
	scan_data->currPixelsRecordReader = nullptr;
}

PixelsFdwExecutionState*
createPixelsFdwExecutionState(string filename,
							  TupleDesc tupleDesc) {
    return new PixelsFdwExecutionState(filename, tupleDesc);
}
