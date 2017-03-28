package hashkit

const (
	FNV_64_INIT  uint64 = 0xcbf29ce484222325
	FNV_64_PRIME uint64 = 0x100000001b3
	FNV_32_INIT  uint32 = 2166136261
	FNV_32_PRIME uint32 = 16777619
)

func HashFnv1_64(buf []byte) (hash uint32) {
	var temp uint64 = FNV_64_INIT
	for _, b := range buf {
		temp *= FNV_64_PRIME
		temp ^= uint64(b)
	}
	hash = uint32(temp)
	return
}

func HashFnv1a_64(buf []byte) (hash uint32) {
	hash = uint32(FNV_64_INIT & 0xFFFFFFFF)
	for _, b := range buf {
		hash ^= uint32(b)
		hash *= uint32(FNV_64_PRIME & 0xFFFFFFFF)
	}
	return
}

func HashFnv1_32(buf []byte) (hash uint32) {
	hash = FNV_32_INIT
	for _, b := range buf {
		val := uint32(b)
		hash *= FNV_32_PRIME
		hash ^= val
	}
	return
}

func HashFnv1a_32(buf []byte) (hash uint32) {
	hash = FNV_32_INIT
	for _, b := range buf {
		val := uint32(b)
		hash ^= val
		hash *= FNV_32_PRIME
	}
	return
}
