package util

import (
	"crypto/rand"
	"encoding/hex"
)

func GenerateID(prefix string) string {
	bytes := make([]byte, 4)
	if _, err := rand.Read(bytes); err != nil {
		panic(err)
	}
	return prefix + hex.EncodeToString(bytes)
}
