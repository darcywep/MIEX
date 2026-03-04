package aria

import "sync"

type Barrier struct {
	n, count int
	gen      int64
	mtx      sync.Mutex
	cond     *sync.Cond
}

func NewBarrier(n int) *Barrier {
	b := &Barrier{n: n, count: n}
	b.cond = sync.NewCond(&b.mtx)
	return b
}

func (b *Barrier) Wait() {
	b.mtx.Lock()
	gen := b.gen
	b.count--
	if b.count == 0 {
		b.gen++
		b.count = b.n
		b.cond.Broadcast()
		b.mtx.Unlock()
		return
	}
	for gen == b.gen {
		b.cond.Wait()
	}
	b.mtx.Unlock()
}
