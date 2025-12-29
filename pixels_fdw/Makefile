MODULE_big = pixels_fdw
OBJS = pixels_fdw.o pixels-cpp/pixels-common/lib/physical/StorageFactory.o pixels-cpp/pixels-common/lib/physical/io/PhysicalLocalReader.o pixels-cpp/pixels-common/lib/physical/allocator/BufferPoolAllocator.o pixels-cpp/pixels-common/lib/physical/Request.o pixels-cpp/pixels-common/lib/physical/RequestBatch.o pixels-cpp/pixels-common/lib/physical/Storage.o pixels-cpp/pixels-common/lib/physical/BufferPool.o pixels-cpp/pixels-common/lib/physical/SchedulerFactory.o pixels-cpp/pixels-common/lib/physical/natives/ByteBuffer.o pixels-cpp/pixels-common/lib/physical/natives/PixelsRandomAccessFile.o pixels-cpp/pixels-common/lib/physical/natives/DirectIoLib.o pixels-cpp/pixels-common/lib/physical/natives/DirectRandomAccessFile.o pixels-cpp/pixels-common/lib/physical/storage/LocalFS.o pixels-cpp/pixels-common/lib/physical/scheduler/NoopScheduler.o pixels-cpp/pixels-common/lib/physical/scheduler/SortMergeScheduler.o pixels-cpp/pixels-common/lib/physical/StorageArrayScheduler.o pixels-cpp/pixels-common/lib/utils/ColumnSizeCSVReader.o pixels-cpp/pixels-common/lib/utils/ConfigFactory.o pixels-cpp/pixels-common/lib/utils/Constants.o pixels-cpp/pixels-common/lib/utils/String.o pixels-cpp/pixels-common/lib/profiler/CountProfiler.o pixels-cpp/pixels-common/lib/profiler/TimeProfiler.o pixels-cpp/pixels-common/lib/MergedRequest.o pixels-cpp/pixels-common/lib/exception/InvalidArgumentException.o PixelsFilter.o PixelsFdwPlanState.o PixelsFdwExecutionState.o pixels-cpp/pixels-proto/pixels.pb.o pixels_impl.o pixels-cpp/pixels-core/lib/TypeDescription.o pixels-cpp/pixels-core/lib/PixelsFooterCache.o pixels-cpp/pixels-core/lib/reader/DateColumnReader.o pixels-cpp/pixels-core/lib/reader/StringColumnReader.o pixels-cpp/pixels-core/lib/reader/ColumnReaderBuilder.o pixels-cpp/pixels-core/lib/reader/PixelsRecordReaderImpl.o pixels-cpp/pixels-core/lib/reader/DecimalColumnReader.o pixels-cpp/pixels-core/lib/reader/IntegerColumnReader.o pixels-cpp/pixels-core/lib/reader/ColumnReader.o pixels-cpp/pixels-core/lib/reader/VarcharColumnReader.o pixels-cpp/pixels-core/lib/reader/PixelsReaderOption.o pixels-cpp/pixels-core/lib/reader/CharColumnReader.o pixels-cpp/pixels-core/lib/reader/TimestampColumnReader.o pixels-cpp/pixels-core/lib/encoding/Decoder.o pixels-cpp/pixels-core/lib/encoding/RunLenIntDecoder.o pixels-cpp/pixels-core/lib/encoding/RunLenIntEncoder.o pixels-cpp/pixels-core/lib/encoding/Encoder.o pixels-cpp/pixels-core/lib/vector/LongColumnVector.o pixels-cpp/pixels-core/lib/vector/TimestampColumnVector.o pixels-cpp/pixels-core/lib/vector/DecimalColumnVector.o pixels-cpp/pixels-core/lib/vector/BinaryColumnVector.o pixels-cpp/pixels-core/lib/vector/VectorizedRowBatch.o pixels-cpp/pixels-core/lib/vector/ByteColumnVector.o pixels-cpp/pixels-core/lib/vector/DateColumnVector.o pixels-cpp/pixels-core/lib/vector/ColumnVector.o pixels-cpp/pixels-core/lib/Category.o pixels-cpp/pixels-core/lib/PixelsBitMask.o pixels-cpp/pixels-core/lib/PixelsVersion.o pixels-cpp/pixels-core/lib/PixelsReaderImpl.o pixels-cpp/pixels-core/lib/PixelsReaderBuilder.o pixels-cpp/pixels-core/lib/utils/EncodingUtils.o pixels-cpp/pixels-core/lib/exception/PixelsFileVersionInvalidException.o pixels-cpp/pixels-core/lib/exception/PixelsFileMagicInvalidException.o pixels-cpp/pixels-core/lib/exception/PixelsReaderException.o 
PGFILEDESC = "pixels_fdw - foreign data wrapper for pixels reader"

SHLIB_LINK = -lm -lstdc++ -L$(PIXELS_FDW_SRC)/third-party/protobuf/cmake/build -lprotobuf 

EXTENSION = pixels_fdw
DATA = pixels_fdw--1.0.sql

override PG_CXXFLAGS += -std=c++17 -I./include -I./pixels-cpp/pixels-common/include -I./pixels-cpp/pixels-core/include -I./pixels-cpp/pixels-proto -I./third-party/protobuf/src -I./third-party/protobuf/cmake/build/include 

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

PROTOBUF_DIR=third-party/protobuf

pull:
	git submodule init
	git submodule update --recursive --init

deps:
	mkdir -p "${PROTOBUF_DIR}/cmake/build" && cd "third-party/protobuf/cmake/build" && \
	cmake -Dprotobuf_BUILD_TESTS=OFF -DCMAKE_BUILD_TYPE=Release ../.. -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
		-Dprotobuf_BUILD_SHARED_LIBS=ON -DCMAKE_INSTALL_PREFIX=./ && \
	make -j16 install

%.bc : %.cpp
	$(COMPILE.cxx.bc) $(CXXFLAGS) $(CPPFLAGS)  -o $@ $<
