package lrmr

type Context interface {
	Broadcast(key string) interface{}
}
