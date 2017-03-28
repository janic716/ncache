package hashkit

import (
	. "ncache/utils"
	"testing"
)

var hashBuf []byte = []byte("Testkey")

const (
	hash_fnv1_32  = 1791109638
	hash_fnv1a_32 = 3361868716
	hash_fnv1_64  = 1725990342
	hash_fnv1a_64 = 1433933132
	hash_crc16    = 3782884451
	hash_crc32    = 15665
	hash_crc32a   = 1026642780
)

func TestHashFnv1_32(t *testing.T) {
	AssertMust(HashFnv1_32(hashBuf) == hash_fnv1_32)
}

func TestHashFnv1a_32(t *testing.T) {
	AssertMust(HashFnv1a_32(hashBuf) == hash_fnv1a_32)
}

func TestHashFnv1_64(t *testing.T) {
	AssertMust(HashFnv1_64(hashBuf) == hash_fnv1_64)
}

func TestHashFnv1a_64(t *testing.T) {
	AssertMust(HashFnv1a_64(hashBuf) == hash_fnv1a_64)
}

func TestHashCrc16(t *testing.T) {
	AssertMust(HashCrc16(hashBuf) == hash_crc16)
}

func TestHashCrc32(t *testing.T) {
	AssertMust(HashCrc32(hashBuf) == hash_crc32)
}

func TestHashCrc32a(t *testing.T) {
	AssertMust(HashCrc32a(hashBuf) == hash_crc32a)
}
