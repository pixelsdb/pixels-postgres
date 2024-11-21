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


template <typename T>
const T Load(void *ptr) {
	T ret;
	memcpy(&ret, ptr, sizeof(ret)); // NOLINT
	return ret;
}

template <typename T>
void Store(const T &val, void *ptr) {
	memcpy(ptr, (void *)&val, sizeof(val)); // NOLINT
}

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
			memset(value.pointer.prefix, 0, PREFIX_BYTES);
		}
	}

	struct StringComparisonOperators {
		static inline bool Equals(const string_t &a, const string_t &b) {
			uint64_t a_bulk_comp = Load<uint64_t>((void*)&a);
			uint64_t b_bulk_comp = Load<uint64_t>((void*)&b);
			if (a_bulk_comp != b_bulk_comp) {
				// Either length or prefix are different -> not equal
				return false;
			}
			// they have the same length and same prefix!
			a_bulk_comp = Load<uint64_t>((void*)(&a) + 8u);
			b_bulk_comp = Load<uint64_t>((void*)(&b) + 8u);
			if (a_bulk_comp == b_bulk_comp) {
				// either they are both inlined (so compare equal) or point to the same string (so compare equal)
				return true;
			}
			if (!a.IsInlined()) {
				// 'long' strings of the same length -> compare pointed value
				if (memcmp(a.value.pointer.ptr, b.value.pointer.ptr, a.GetSize()) == 0) {
					return true;
				}
			}
			// either they are short string of same length but different content
			//     or they point to string with different content
			//     either way, they can't represent the same underlying string
			return false;
		}
		// compare up to shared length. if still the same, compare lengths
		static bool GreaterThan(const string_t &left, const string_t &right) {
			const uint32_t left_length = static_cast<uint32_t>(left.GetSize());
			const uint32_t right_length = static_cast<uint32_t>(right.GetSize());
			const uint32_t min_length = std::min<uint32_t>(left_length, right_length);

#ifndef DUCKDB_DEBUG_NO_INLINE
			uint32_t a_prefix = Load<uint32_t>((void*)(left.GetPrefix()));
			uint32_t b_prefix = Load<uint32_t>((void*)(right.GetPrefix()));

			// Utility to move 0xa1b2c3d4 into 0xd4c3b2a1, basically inverting the order byte-a-byte
			auto byte_swap = [](uint32_t v) -> uint32_t {
				uint32_t t1 = (v >> 16u) | (v << 16u);
				uint32_t t2 = t1 & 0x00ff00ff;
				uint32_t t3 = t1 & 0xff00ff00;
				return (t2 << 8u) | (t3 >> 8u);
			};

			// Check on prefix -----
			// We dont' need to mask since:
			//	if the prefix is greater(after bswap), it will stay greater regardless of the extra bytes
			// 	if the prefix is smaller(after bswap), it will stay smaller regardless of the extra bytes
			//	if the prefix is equal, the extra bytes are guaranteed to be /0 for the shorter one

			if (a_prefix != b_prefix) {
				return byte_swap(a_prefix) > byte_swap(b_prefix);
			}
#endif
			auto memcmp_res = memcmp(left.GetData(), right.GetData(), min_length);
			return memcmp_res > 0 || (memcmp_res == 0 && left_length > right_length);
		}
	};

	bool operator==(const string_t &r) const {
		return StringComparisonOperators::Equals(*this, r);
	}

	bool operator!=(const string_t &r) const {
		return !(*this == r);
	}

	bool operator>(const string_t &r) const {
		return StringComparisonOperators::GreaterThan(*this, r);
	}
	bool operator<(const string_t &r) const {
		return r > *this;
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