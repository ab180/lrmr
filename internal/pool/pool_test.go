package pool

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPool(t *testing.T) {
	r := require.New(t)

	var cnt atomic.Int64
	p := New[[]int](func() []int {
		cnt.Add(1)
		return nil
	})

	it1 := p.GetReleasable()
	r.Equal(int64(1), cnt.Load())
	r.Nil(it1.Value())

	p.Put([]int{1, 2, 3})
	it2 := p.GetReleasable()
	r.Equal(int64(1), cnt.Load())
	r.EqualValues([]int{1, 2, 3}, it2.Value())

	it3 := p.GetReleasable()
	r.Equal(int64(2), cnt.Load())
	r.Nil(it3.Value())

	it2.Release()
	it4 := p.GetReleasable()
	r.Equal(int64(2), cnt.Load())
	r.EqualValues([]int{1, 2, 3}, it4.Value())
}

func TestPoolWithResetter(t *testing.T) {
	r := require.New(t)

	var newCnt, resetCnt atomic.Int64
	p := NewWithResetter[[]int](func() []int {
		defer newCnt.Add(1)
		return nil
	}, func(_v *[]int) {
		defer resetCnt.Add(1)
		v := *_v
		for i := range v {
			v[i] = 0
		}
	})

	it1 := p.GetReleasable()
	r.Equal(int64(1), newCnt.Load())
	r.Nil(it1.Value())

	p.Put([]int{1, 2, 3})
	r.Equal(int64(0), resetCnt.Load())

	it2 := p.GetReleasable()
	r.Equal(int64(1), newCnt.Load())
	r.EqualValues([]int{1, 2, 3}, it2.Value())

	it3 := p.GetReleasable()
	r.Equal(int64(2), newCnt.Load())
	r.Equal(int64(0), resetCnt.Load())
	r.Nil(it3.Value())

	it2.ResetAndRelease()
	r.Equal(int64(1), resetCnt.Load())

	it4 := p.GetReleasable()
	r.Equal(int64(2), newCnt.Load())
	r.EqualValues([]int{0, 0, 0}, it4.Value())

	it4.Value()[0] = 1
	it4.Release()
	r.Equal(int64(1), resetCnt.Load())

	it5 := p.GetReleasable()
	r.Equal(int64(2), newCnt.Load())
	r.EqualValues([]int{1, 0, 0}, it5.Value())
}

func benchmarkPoolNbyte(b *testing.B, n int) {
	newB := func() []byte {
		return make([]byte, n)
	}
	b.ResetTimer()
	b.Run("no pool", func(b *testing.B) {
		var wg sync.WaitGroup
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				data := newB()
				_ = data
			}()
		}
		wg.Wait()
	})
	b.Run("sync.Pool", func(b *testing.B) {
		var wg sync.WaitGroup
		p := sync.Pool{
			New: func() any {
				return newB()
			},
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				data := p.Get().([]byte)
				_ = data
				p.Put(data)
			}()
		}
		wg.Wait()
	})
	b.Run("pool.pool", func(b *testing.B) {
		var wg sync.WaitGroup
		p := New[[]byte](newB)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				data := p.GetReleasable()
				_ = data
				data.Release()
			}()
		}
		wg.Wait()
	})
}

func BenchmarkPool(b *testing.B) {
	b.Run("1kib", func(b *testing.B) {
		benchmarkPoolNbyte(b, 1024)
	})
	b.Run("1mib", func(b *testing.B) {
		benchmarkPoolNbyte(b, 1024*1024)
	})
	b.Run("5mib", func(b *testing.B) {
		benchmarkPoolNbyte(b, 5*1024*1024)
	})
}
