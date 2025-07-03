package util

import (
	"crypto/sha1"
	"encoding/hex"
)

// Hash 使用 SHA-1 哈希函数将输入转换为散列值
func Hash(input string) string {
	// 使用 SHA-1 哈希函数
	h := sha1.New()
	h.Write([]byte(input))
	// 返回十六进制编码的哈希值
	return hex.EncodeToString(h.Sum(nil))
}
