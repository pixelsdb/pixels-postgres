#include "PixelsReader.h"


struct PixelsRelMetaData {
	std::shared_ptr<PixelsReader> initialPixelsReader;
	std::vector<std::string> files;
};