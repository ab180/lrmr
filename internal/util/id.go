package util

import (
	"crypto/rand"
	"encoding/hex"
)

func GenerateID(prefix string) string {
	bytes := make([]byte, 8)
	if _, err := rand.Read(bytes); err != nil {
		panic(err)
	}
	return prefix + hex.EncodeToString(bytes)
}
