//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/string_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstring>
#include <algorithm>
#include <cstdint>
#include <cassert>
#include <string>

struct string_t {

public:
	static constexpr uint32_t PREFIX_BYTES = 4 * sizeof(char);
	static constexpr uint32_t INLINE_BYTES = 12 * sizeof(char);
	static constexpr uint32_t HEADER_SIZE = sizeof(uint32_t) + PREFIX_BYTES;
	static constexpr uint32_t MAX_STRING_SIZE = UINT32_MAX;
	static constexpr uint32_t PREFIX_LENGTH = PREFIX_BYTES;
	static constexpr uint32_t INLINE_LENGTH = INLINE_BYTES;

	string_t() = default;
	explicit string_t(uint32_t len) {
		value.inlined.length = len;
	}
	string_t(const char *data, uint32_t len) {
		value.inlined.length = len;
		assert(data || GetSize() == 0);
		if (IsInlined()) {
			// zero initialize the prefix first
			// this makes sure that strings with length smaller than 4 still have an equal prefix
			memset(value.inlined.inlined, 0, INLINE_BYTES);
			if (GetSize() == 0) {
				return;
			}
			// small string: inlined
			memcpy(value.inlined.inlined, data, GetSize());
		} else {
			// large string: store pointer
			memcpy(value.pointer.prefix, data, PREFIX_LENGTH);
			value.pointer.ptr = (char *)data; // NOLINT
		}
	}

	string_t(const char *data) // NOLINT: Allow implicit conversion from `const char*`
	    : string_t(data, static_cast<uint32_t>(strlen(data))) {
	}
	string_t(const std::string &value) // NOLINT: Allow implicit conversion from `const char*`
	    : string_t(value.c_str(), static_cast<uint32_t>(value.size())) {
	}

	bool IsInlined() const {
		return GetSize() <= INLINE_LENGTH;
	}

	const char *GetData() const {
		return IsInlined() ? const_cast<char*>(value.inlined.inlined) : value.pointer.ptr;
	}
	const char *GetDataUnsafe() const {
		return GetData();
	}

	char *GetDataWriteable() const {
		return IsInlined() ? (char *)value.inlined.inlined : value.pointer.ptr; // NOLINT
	}

	const char *GetPrefix() const {
		return value.inlined.inlined;
	}

	char *GetPrefixWriteable() {
		return value.inlined.inlined;
	}

	uint32_t GetSize() const {
		return value.inlined.length;
	}

	bool Empty() const {
		return value.inlined.length == 0;
	}

	std::string GetString() const {
		return std::string(GetData(), GetSize());
	}

	void Finalize() {
		// set trailing NULL byte
		if (GetSize() <= INLINE_LENGTH) {
			// fill prefix with zeros if the length is smaller than the prefix length
			memset(value.inlined.inlined + GetSize(), 0, INLINE_BYTES - GetSize());
		} else {
			// copy the data into the prefix
#ifndef DUCKDB_DEBUG_NO_INLINE
			auto dataptr = GetData();
			memcpy(value.pointer.prefix, dataptr, PREFIX_LENGTH);
#else
			memset(value.pointer.prefix, 0, PREFIX_BYTES);
#endif
		}
	}
private:
	union {
		struct {
			uint32_t length;
			char prefix[4];
			char *ptr;
		} pointer;
		struct {
			uint32_t length;
			char inlined[12];
		} inlined;
	} value;
};