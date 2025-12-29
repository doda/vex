package vector

import "math"

// Float16ToFloat32 converts a float16 (half-precision) to float32.
func Float16ToFloat32(h uint16) float32 {
	sign := uint32((h >> 15) & 1)
	exp := int32((h >> 10) & 0x1F)
	mant := uint32(h & 0x3FF)

	var f uint32
	switch exp {
	case 0:
		if mant == 0 {
			f = sign << 31
		} else {
			for mant&0x400 == 0 {
				mant <<= 1
				exp--
			}
			exp++
			mant &= 0x3FF
			f = (sign << 31) | (uint32(exp+127-15) << 23) | (mant << 13)
		}
	case 31:
		f = (sign << 31) | 0x7F800000 | (mant << 13)
	default:
		f = (sign << 31) | (uint32(exp+127-15) << 23) | (mant << 13)
	}

	return math.Float32frombits(f)
}

// Float32ToFloat16 converts a float32 to float16 (half-precision).
func Float32ToFloat16(f float32) uint16 {
	bits := math.Float32bits(f)
	sign := uint16((bits >> 16) & 0x8000)
	exp := int32((bits >> 23) & 0xFF)
	mant := bits & 0x7FFFFF

	switch exp {
	case 0xFF:
		if mant == 0 {
			return sign | 0x7C00
		}
		return sign | 0x7C00 | uint16(mant>>13) | 1
	case 0:
		return sign
	}

	exp = exp - 127 + 15
	if exp >= 0x1F {
		return sign | 0x7C00
	}
	if exp <= 0 {
		if exp < -10 {
			return sign
		}
		mant |= 0x800000
		shift := uint32(1 - exp)
		mant16 := mant >> (shift + 13)
		if (mant>>(shift+12))&1 == 1 {
			mant16++
		}
		return sign | uint16(mant16)
	}

	mant16 := mant >> 13
	if mant&0x1000 != 0 {
		mant16++
		if mant16 == 0x400 {
			mant16 = 0
			exp++
			if exp >= 0x1F {
				return sign | 0x7C00
			}
		}
	}

	return sign | uint16(exp<<10) | uint16(mant16)
}
