package pool

import (
	"sync"
)

type pool[T any] struct {
	p        sync.Pool
	resetter func(*T)
}

func New[T any](constructor func() T) *pool[T] {
	return NewWithResetter(constructor, nil)
}

func NewWithResetter[T any](constructor func() T, resetter func(*T)) *pool[T] {
	return &pool[T]{
		p: sync.Pool{
			New: func() interface{} {
				return constructor()
			},
		},
		resetter: resetter,
	}
}

func (p *pool[T]) Get() T {
	return p.p.Get().(T)
}

func (p *pool[T]) GetReleasable() Releasable[T] {
	return Releasable[T]{
		p:     p,
		value: p.p.Get().(T),
	}
}

func (p *pool[T]) ResetAndPut(v T) {
	p.resetter(&v)
	p.Put(v)
}

func (p *pool[T]) Put(v T) {
	p.p.Put(v)
}

type Releasable[T any] struct {
	p     *pool[T]
	value T
}

func (r Releasable[T]) Value() T {
	return r.value
}

func (r Releasable[T]) ResetAndRelease() {
	r.p.resetter(&r.value)
	r.Release()
}

func (r Releasable[T]) Release() {
	r.p.p.Put(r.value)
}
