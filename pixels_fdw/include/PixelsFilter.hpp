//
// Created by liyu on 6/23/23.
//
#pragma once

#include <bitset>
#include "PixelsBitMask.h"
#include "vector/ColumnVector.h"
#include "TypeDescription.h"
#include "string_t.hpp"
#include <cmath>
#include <immintrin.h>
#include <avxintrin.h>

enum class PixelsFilterType : uint8_t {
	CONJUNCTION_AND = 0,
	CONJUNCTION_OR,
    COMPARE_EQ,
    COMPARE_GTEQ,
    COMPARE_LTEQ,
    COMPARE_GT,
    COMPARE_LT
};

class PixelsFilterOp {
public:
    struct Equals {
	    template <class T>
	    static inline bool Operation(const T &left, const T &right) {
		    return left == right;
	    }
    };

    struct GreaterThan {
	    template <class T>
	    static inline bool Operation(const T &left, const T &right) {
		    return left > right;
	    }
    };

    struct GreaterThanEquals {
	    template <class T>
	    static inline bool Operation(const T &left, const T &right) {
		    return !GreaterThan::Operation(right, left);
	    }
    };

    struct LessThan {
	    template <class T>
	    static inline bool Operation(const T &left, const T &right) {
		    return GreaterThan::Operation(right, left);
	    }
    };

    struct LessThanEquals {
	    template <class T>
	    static inline bool Operation(const T &left, const T &right) {
		    return !GreaterThan::Operation(left, right);
	    }
    };
};

class PixelsFilter {
public:
    PixelsFilter(PixelsFilterType type,
                 std::string cname,
                 long ivalue,
                 double dvalue,
                 string_t svalue);
    ~PixelsFilter();
    std::string getColumnName();
    PixelsFilterType getFilterType();
    void setColumnName(std::string cname);
    long getIntegerValue();
    double getDecimalValue();
    string_t getStringValue();
    PixelsFilter *getLChild();
    void setLChild(PixelsFilter *lc);
    PixelsFilter *getRChild();
    void setRChild(PixelsFilter *rc);
    PixelsFilter *copy();
    void ApplyFilter(std::shared_ptr<ColumnVector> vector,
                     PixelsBitMask& filterMask,
                     std::shared_ptr<TypeDescription> type);
    template <class T, class OP>
    static int CompareAvx2(void * data, T constant);
    template <class OP>
    static void TemplatedFilterOperation(std::shared_ptr<ColumnVector> vector,
                                         const long ivalue,
                                         const double dvalue,
                                         const string_t svalue,
                                         PixelsBitMask &filter_mask,
                                         std::shared_ptr<TypeDescription> type);

    template <class OP>
    static void FilterOperationSwitch(std::shared_ptr<ColumnVector> vector,
                                      const long ivalue,
                                      const double dvalue,
                                      const string_t svalue,
                                      PixelsBitMask &filter_mask,
                                      std::shared_ptr<TypeDescription> type);
private:
    PixelsFilterType pixelsFilterType;
    std::string column_name;
    long integer_value;
    double decimal_value;
    string_t string_value;
    PixelsFilter *lchild = nullptr;
    PixelsFilter *rchild = nullptr;
};

PixelsFilter* createPixelsFilter(PixelsFilterType type,
                                 std::string cname,
                                 const long ivalue,
                                 const double dvalue,
                                 string_t svalue);
