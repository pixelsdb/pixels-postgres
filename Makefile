MODULE_big = pixels_fdw
OBJS = pixels_fdw.o pixels-common/lib/physical/StorageFactory.o pixels-common/lib/physical/io/PhysicalLocalReader.o pixels-common/lib/physical/allocator/BufferPoolAllocator.o pixels-common/lib/physical/Request.o pixels-common/lib/physical/RequestBatch.o pixels-common/lib/physical/Storage.o pixels-common/lib/physical/BufferPool.o pixels-common/lib/physical/SchedulerFactory.o pixels-common/lib/physical/natives/ByteBuffer.o pixels-common/lib/physical/natives/PixelsRandomAccessFile.o pixels-common/lib/physical/natives/DirectIoLib.o pixels-common/lib/physical/natives/DirectRandomAccessFile.o pixels-common/lib/physical/storage/LocalFS.o pixels-common/lib/physical/scheduler/NoopScheduler.o pixels-common/lib/physical/scheduler/SortMergeScheduler.o pixels-common/lib/physical/StorageArrayScheduler.o pixels-common/lib/utils/ColumnSizeCSVReader.o pixels-common/lib/utils/ConfigFactory.o pixels-common/lib/utils/Constants.o pixels-common/lib/utils/String.o pixels-common/lib/profiler/CountProfiler.o pixels-common/lib/profiler/TimeProfiler.o pixels-common/lib/MergedRequest.o pixels-common/lib/exception/InvalidArgumentException.o PixelsFdwPlanState.o PixelsFdwExecutionState.o pixels-proto/pixels.pb.o pixels_impl.o pixels-core/lib/TypeDescription.o pixels-core/lib/PixelsFooterCache.o pixels-core/lib/reader/DateColumnReader.o pixels-core/lib/reader/StringColumnReader.o pixels-core/lib/reader/ColumnReaderBuilder.o pixels-core/lib/reader/PixelsRecordReaderImpl.o pixels-core/lib/reader/DecimalColumnReader.o pixels-core/lib/reader/IntegerColumnReader.o pixels-core/lib/reader/ColumnReader.o pixels-core/lib/reader/VarcharColumnReader.o pixels-core/lib/reader/PixelsReaderOption.o pixels-core/lib/reader/CharColumnReader.o pixels-core/lib/reader/TimestampColumnReader.o pixels-core/lib/encoding/Decoder.o pixels-core/lib/encoding/RunLenIntDecoder.o pixels-core/lib/encoding/RunLenIntEncoder.o pixels-core/lib/encoding/Encoder.o pixels-core/lib/vector/LongColumnVector.o pixels-core/lib/vector/TimestampColumnVector.o pixels-core/lib/vector/DecimalColumnVector.o pixels-core/lib/vector/BinaryColumnVector.o pixels-core/lib/vector/VectorizedRowBatch.o pixels-core/lib/vector/ByteColumnVector.o pixels-core/lib/vector/DateColumnVector.o pixels-core/lib/vector/ColumnVector.o pixels-core/lib/Category.o pixels-core/lib/PixelsVersion.o pixels-core/lib/PixelsReaderImpl.o pixels-core/lib/PixelsReaderBuilder.o pixels-core/lib/utils/EncodingUtils.o pixels-core/lib/exception/PixelsFileVersionInvalidException.o pixels-core/lib/exception/PixelsFileMagicInvalidException.o pixels-core/lib/exception/PixelsReaderException.o 
PGFILEDESC = "pixels_fdw - foreign data wrapper for pixels reader"

SHLIB_LINK = -lm -lstdc++ -L$(PIXELS_FDW_SRC)./third-party/protobuf/cmake/build -lprotobuf 

EXTENSION = pixels_fdw
DATA = pixels_fdw--1.0.sql

override PG_CXXFLAGS += -std=c++17 -I./include -I./pixels-common/include -I./pixels-core/include -I./pixels-proto -I./third-party/protobuf/src

ifdef CCFLAGS
	override PG_CXXFLAGS += $(CCFLAGS)
	override PG_CFLAGS += $(CCFLAGS)
endif

COMPILE.cxx.bc = $(CLANG) -xc++ -Wno-ignored-attributes -Wno-register $(BITCODE_CXXFLAGS) $(CPPFLAGS) -emit-llvm -c

REGRESS = pixels_fdw

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/pixels_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

%.bc : %.cpp
	$(COMPILE.cxx.bc) $(CXXFLAGS) $(CPPFLAGS)  -o $@ $<