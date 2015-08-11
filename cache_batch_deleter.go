package appx

import (
	"appengine"
	"appengine/memcache"
	"github.com/drborges/riversv2/rx"
)

type cacheBatchDeleter struct {
	context appengine.Context
	size    int
	ids     []string
}

func (batch *cacheBatchDeleter) Full() bool {
	return len(batch.ids) == batch.size
}

func (batch *cacheBatchDeleter) Empty() bool {
	return len(batch.ids) == 0
}

func (batch *cacheBatchDeleter) Commit(out rx.OutStream) {
	memcache.DeleteMulti(batch.context, batch.ids)
	batch.ids = []string{}
}

func (batch *cacheBatchDeleter) Add(data rx.T) {
	if cacheable, ok := data.(Cacheable); ok {
		batch.ids = append(batch.ids, cacheable.CacheID())
	}
}
