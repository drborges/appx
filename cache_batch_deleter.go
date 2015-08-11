package appx

import (
	"appengine"
	"appengine/memcache"
	"github.com/drborges/riversv2/rx"
)

type CacheBatchDeleter struct {
	context appengine.Context
	size    int
	ids     []string
}

func NewCacheBatchDeleterWithSize(context appengine.Context, size int) *CacheBatchDeleter {
	return &CacheBatchDeleter{
		context: context,
		size:    size,
	}
}

func (batch *CacheBatchDeleter) Full() bool {
	return len(batch.ids) == batch.size
}

func (batch *CacheBatchDeleter) Empty() bool {
	return len(batch.ids) == 0
}

func (batch *CacheBatchDeleter) Commit(out rx.OutStream) {
	memcache.DeleteMulti(batch.context, batch.ids)
	batch.ids = []string{}
}

func (batch *CacheBatchDeleter) Add(data rx.T) {
	if cacheable, ok := data.(Cacheable); ok {
		batch.ids = append(batch.ids, cacheable.CacheID())
	}
}
