#include <bitset>
#include "vector/ColumnVector.h"
#include "TypeDescription.h"

class PixelsBitMask {
public:
    uint8_t * mask;
    long maskLength;
    long arrayLength;
    PixelsBitMask(long length);
    PixelsBitMask(PixelsBitMask & other);
    ~PixelsBitMask();
    void Or(PixelsBitMask & other);
    void And(PixelsBitMask & other);
    void Or(long index, uint8_t value);
    void And(long index, uint8_t value);
    bool isNone();
    void set();
    void set(long index, uint8_t value);
    void setByteAligned(long index, uint8_t value);
    uint8_t get(long index);
};