package util

import (
	"strconv"
	"unsafe"
)

// 字符串转换函数
func BytesToString(b []byte) string {
	// 使用unsafe进行高性能转换 (与go-redis保持一致)
	return *(*string)(unsafe.Pointer(&b))
}

func StringToBytes(s string) []byte {
	// 使用unsafe进行高性能转换 (与go-redis保持一致)
	return *(*[]byte)(unsafe.Pointer(
		&struct {
			string
			Cap int
		}{s, len(s)},
	))
}

// 数字解析函数 (proto包需要的所有函数)
func Atoi(b []byte) (int, error) {
	return strconv.Atoi(BytesToString(b))
}

func ParseInt(b []byte, base int, bitSize int) (int64, error) {
	return strconv.ParseInt(BytesToString(b), base, bitSize)
}

func ParseUint(b []byte, base int, bitSize int) (uint64, error) {
	return strconv.ParseUint(BytesToString(b), base, bitSize)
}

func ParseFloat(b []byte, bitSize int) (float64, error) {
	return strconv.ParseFloat(BytesToString(b), bitSize)
}
