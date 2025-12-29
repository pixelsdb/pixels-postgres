
#include "PixelsFilter.hpp"


PixelsFilter::PixelsFilter(PixelsFilterType type,
                           std::string cname,
                           long ivalue,
                           double dvalue,
                           string_t svalue) {
    pixelsFilterType = type;
    column_name = cname;
    integer_value = ivalue;
    decimal_value = dvalue;
    string_value = svalue;
}

PixelsFilter::~PixelsFilter() {
    if (lchild) {
        delete lchild;
    }
    if (rchild) {
        delete rchild;
    }
}

std::string PixelsFilter::getColumnName() {
    return column_name;
}

PixelsFilterType PixelsFilter::getFilterType() {
    return pixelsFilterType;
}

void PixelsFilter::setColumnName(std::string cname) {
    column_name = cname;
}

long PixelsFilter::getIntegerValue()
{
    return integer_value;
}

double PixelsFilter::getDecimalValue() {
    return decimal_value;
}

string_t PixelsFilter::getStringValue() {
    return string_value;
}

PixelsFilter *PixelsFilter::getLChild() {
    return lchild;
}

void PixelsFilter::setLChild(PixelsFilter *lc) {
    lchild = lc;
}

PixelsFilter *PixelsFilter::getRChild() {
    return rchild;
}

void PixelsFilter::setRChild(PixelsFilter *rc) {
    rchild = rc;
}

PixelsFilter *PixelsFilter::copy() {
    return new PixelsFilter(pixelsFilterType, column_name, integer_value, decimal_value, string_value);
}

template<class T, class OP>
int PixelsFilter::CompareAvx2(void * data,
                              T constant) {
    __m256i vector;
    __m256i vector_next;
    __m256i constants;
    __m256i mask;
    if constexpr(sizeof(T) == 4) {
        vector = _mm256_load_si256((__m256i *)data);
        constants = _mm256_set1_epi32(constant);
        if constexpr(std::is_same<OP, PixelsFilterOp::Equals>()) {
            mask = _mm256_cmpeq_epi32(vector, constants);
            return _mm256_movemask_ps((__m256)mask);
        } else if constexpr(std::is_same<OP, PixelsFilterOp::LessThan>()) {
            mask = _mm256_cmpgt_epi32(constants, vector);
            return _mm256_movemask_ps((__m256)mask);
        } else if constexpr(std::is_same<OP, PixelsFilterOp::LessThanEquals>()) {
            mask = _mm256_cmpgt_epi32(vector, constants);
            return ~_mm256_movemask_ps((__m256)mask);
        } else if constexpr(std::is_same<OP, PixelsFilterOp::GreaterThan>()) {
            mask = _mm256_cmpgt_epi32(vector, constants);
            return _mm256_movemask_ps((__m256)mask);
        } else if constexpr(std::is_same<OP, PixelsFilterOp::GreaterThanEquals>()) {
            mask = _mm256_cmpgt_epi32(constants, vector);
            return ~_mm256_movemask_ps((__m256)mask);
        }
    } else if constexpr(sizeof(T) == 8) {
        constants = _mm256_set1_epi64x(constant);
        vector = _mm256_load_si256((__m256i *)data);
        vector_next = _mm256_load_si256((__m256i *)((uint8_t *)data + 32));
        int result = 0;
        if constexpr(std::is_same<OP, PixelsFilterOp::Equals>()) {
            mask = _mm256_cmpeq_epi64(vector, constants);
            result = _mm256_movemask_pd((__m256d)mask);
            mask = _mm256_cmpeq_epi64(vector_next, constants);
            result += _mm256_movemask_pd((__m256d)mask) << 4;
            return result;
        } else if constexpr(std::is_same<OP, PixelsFilterOp::LessThan>()) {
            mask = _mm256_cmpgt_epi64(constants, vector);
            result = _mm256_movemask_pd((__m256d)mask);
            mask = _mm256_cmpgt_epi64(constants, vector_next);
            result += _mm256_movemask_pd((__m256d)mask) << 4;
            return result;
        } else if constexpr(std::is_same<OP, PixelsFilterOp::LessThanEquals>()) {
            mask = _mm256_cmpgt_epi64(vector, constants);
            result = _mm256_movemask_pd((__m256d)mask);
            mask = _mm256_cmpgt_epi64(vector_next, constants);
            result += _mm256_movemask_pd((__m256d)mask) << 4;
            return ~result;
        } else if constexpr(std::is_same<OP, PixelsFilterOp::GreaterThan>()) {
            mask = _mm256_cmpgt_epi64(vector, constants);
            result = _mm256_movemask_pd((__m256d)mask);
            mask = _mm256_cmpgt_epi64(vector_next, constants);
            result += _mm256_movemask_pd((__m256d)mask) << 4;
            return result;
        } else if constexpr(std::is_same<OP, PixelsFilterOp::GreaterThanEquals>()) {
            mask = _mm256_cmpgt_epi64(constants, vector);
            result = _mm256_movemask_pd((__m256d)mask);
            mask = _mm256_cmpgt_epi64(constants, vector_next);
            result += _mm256_movemask_pd((__m256d)mask) << 4;
            return ~result;
        }
    } else {
        throw InvalidArgumentException("We didn't support other sizes yet to do filter SIMD");
    }
}


template <class OP>
void PixelsFilter::TemplatedFilterOperation(std::shared_ptr<ColumnVector> vector,
                                            const long ivalue,
                                            const double dvalue,
                                            const string_t svalue,
                                            PixelsBitMask &filter_mask,
                                            std::shared_ptr<TypeDescription> type) {
    switch (type->getCategory()) {
        case TypeDescription::SHORT:
        case TypeDescription::INT: {
            int constant_value = (int)ivalue;
            auto longColumnVector = std::static_pointer_cast<LongColumnVector>(vector);
            int i = 0;
#ifdef  ENABLE_SIMD_FILTER
            for (; i < vector->length - vector->length % 8; i += 8) {
                uint8_t mask = CompareAvx2<uint32_t, OP>(longColumnVector->intVector + i, constant_value);
                filter_mask.setByteAligned(i, mask);
            }
#endif
            for (; i < vector->length; i++) {
                filter_mask.set(i, OP::Operation(longColumnVector->intVector[i],
                                                                 constant_value));
            }
            break;
        }
        case TypeDescription::LONG: {
            long constant_value = (long)ivalue;
            auto longColumnVector = std::static_pointer_cast<LongColumnVector>(vector);
            int i = 0;
#ifdef ENABLE_SIMD_FILTER
            for (; i < vector->length - vector->length % 8; i += 8) {
                uint8_t mask = CompareAvx2<uint64_t, OP>(longColumnVector->longVector + i, constant_value);
                filter_mask.setByteAligned(i, mask);
            }
#endif
            for(; i < vector->length; i++) {
                filter_mask.set(i, OP::Operation(longColumnVector->longVector[i],
                                                 constant_value));
            }
            break;
        }
        case TypeDescription::DATE: {
            int constant_value = (int)ivalue;
            auto dateColumnVector = std::static_pointer_cast<DateColumnVector>(vector);
            int i = 0;
#ifdef ENABLE_SIMD_FILTER
            for (; i < vector->length - vector->length % 8; i += 8) {
                uint8_t mask = CompareAvx2<uint32_t, OP>(dateColumnVector->dates + i, constant_value);
                filter_mask.setByteAligned(i, mask);
            }
#endif
            for (; i < vector->length; i++) {
                filter_mask.set(i, OP::Operation(dateColumnVector->dates[i],
                                                                 constant_value));
            }
            break;
        }
        case TypeDescription::DECIMAL: {
            auto decimalColumnVector = std::static_pointer_cast<DecimalColumnVector>(vector);
            int scale = decimalColumnVector->getScale();
            double decimal_value = dvalue * std::pow(10, scale);
            long long_value = std::lround(decimal_value);
            int i = 0;
#ifdef ENABLE_SIMD_FILTER
            for (; i < vector->length - vector->length % 8; i += 8) {
                uint8_t mask = CompareAvx2<uint64_t, OP>(decimalColumnVector->vector + i, constant_value);
                filter_mask.setByteAligned(i, mask);
            }
#endif
            for (; i < vector->length; i++) {
                filter_mask.set(i, OP::Operation(decimalColumnVector->vector[i],
                                                                 long_value));
            }
            break;
        }
        case TypeDescription::STRING:
        case TypeDescription::BINARY:
        case TypeDescription::VARBINARY:
        case TypeDescription::CHAR:
        case TypeDescription::VARCHAR: {
            string_t constant_value = svalue;
            auto binaryColumnVector = std::static_pointer_cast<BinaryColumnVector>(vector);
            for (int i = 0; i < vector->length; i++) {
                filter_mask.set(i, OP::Operation(binaryColumnVector->vector[i],
                                                                   constant_value));
            }
            break;
        }
    }
}

template <class OP>
void PixelsFilter::FilterOperationSwitch(std::shared_ptr<ColumnVector> vector,
                                         const long ivalue,
                                         const double dvalue,
                                         const string_t svalue,
                                         PixelsBitMask &filter_mask,
                                         std::shared_ptr<TypeDescription> type) {
    if (filter_mask.isNone()) {
        return;
    }
    switch (type->getCategory()) {
        case TypeDescription::SHORT:
        case TypeDescription::INT:
        case TypeDescription::DATE:
            TemplatedFilterOperation<OP>(vector, ivalue, dvalue, svalue, filter_mask, type);
            break;
        case TypeDescription::LONG:
            TemplatedFilterOperation<OP>(vector, ivalue, dvalue, svalue, filter_mask, type);
            break;
        case TypeDescription::DECIMAL:
            TemplatedFilterOperation<OP>(vector, ivalue, dvalue, svalue, filter_mask, type);
            break;
        case TypeDescription::STRING:
        case TypeDescription::BINARY:
        case TypeDescription::VARBINARY:
        case TypeDescription::CHAR:
        case TypeDescription::VARCHAR:
            TemplatedFilterOperation<OP>(vector, ivalue, dvalue, svalue, filter_mask, type);
            break;
        default:
            throw InvalidArgumentException("Unsupported type for filter. ");
    }
}

void
PixelsFilter::ApplyFilter(std::shared_ptr<ColumnVector> vector,
                          PixelsBitMask& filterMask,
                          std::shared_ptr<TypeDescription> type) {
    switch (pixelsFilterType) {
        case PixelsFilterType::CONJUNCTION_AND: {
            if (lchild) {
                PixelsBitMask lchildMask(filterMask.maskLength);
                lchild->ApplyFilter(vector, lchildMask, type);
                filterMask.And(lchildMask);
            }
            if (rchild) {
                PixelsBitMask lchildMask(filterMask.maskLength);
                lchild->ApplyFilter(vector, lchildMask, type);
                filterMask.And(lchildMask);
            }
            break;
        }
        case PixelsFilterType::CONJUNCTION_OR: {
            PixelsBitMask orMask(filterMask.maskLength);
            if (lchild) {
                PixelsBitMask lchildMask(filterMask.maskLength);
                lchild->ApplyFilter(vector, lchildMask, type);
                orMask.Or(lchildMask);
            }
            if (rchild) {
                PixelsBitMask lchildMask(filterMask.maskLength);
                lchild->ApplyFilter(vector, lchildMask, type);
                orMask.Or(lchildMask);
            }
            filterMask.And(orMask);
            break;
        }
        case PixelsFilterType::COMPARE_EQ: {
            FilterOperationSwitch<PixelsFilterOp::Equals>(vector, integer_value, decimal_value, string_value, filterMask, type);
            break;
        }
        case PixelsFilterType::COMPARE_GTEQ: {
            FilterOperationSwitch<PixelsFilterOp::GreaterThanEquals>(vector, integer_value, decimal_value, string_value, filterMask, type);
            break;
        }
        case PixelsFilterType::COMPARE_LTEQ: {
            FilterOperationSwitch<PixelsFilterOp::LessThanEquals>(vector, integer_value, decimal_value, string_value, filterMask, type);
            break;
        }
        case PixelsFilterType::COMPARE_GT: {
            FilterOperationSwitch<PixelsFilterOp::GreaterThan>(vector, integer_value, decimal_value, string_value, filterMask, type);
            break;
        }
        case PixelsFilterType::COMPARE_LT: {
            FilterOperationSwitch<PixelsFilterOp::LessThan>(vector, integer_value, decimal_value, string_value, filterMask, type);
            break;
        }
        default:
            assert(0);
            break;
    }
}

PixelsFilter *createPixelsFilter(PixelsFilterType type,
                                 std::string cname,
                                 long ivalue,
                                 double dvalue,
                                 string_t svalue) {
    return new PixelsFilter(type, cname, ivalue, dvalue, svalue);
}
