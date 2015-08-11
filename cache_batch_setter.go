package appx

import (
	"appengine"
	"appengine/memcache"
	"encoding/json"
	"github.com/drborges/riversv2/rx"
)

type CacheBatchSetter struct {
	context appengine.Context
	size    int
	items   []*memcache.Item
}

func NewCacheBatchSetterWithSize(context appengine.Context, size int) *CacheBatchSetter {
	return &CacheBatchSetter{
		context: context,
		size:    size,
		items:   []*memcache.Item{},
	}
}

func (batch *CacheBatchSetter) Full() bool {
	return len(batch.items) == batch.size
}

func (batch *CacheBatchSetter) Empty() bool {
	return len(batch.items) == 0
}

func (batch *CacheBatchSetter) Commit(out rx.OutStream) {
	if batch.Empty() {
		return
	}

	if err := memcache.SetMulti(batch.context, batch.items); err != nil {
		panic(err)
	}

	batch.items = []*memcache.Item{}
}

func (batch *CacheBatchSetter) Add(data rx.T) {
	if cacheable, ok := data.(Cacheable); ok {
		entity := cacheable.(Entity)
		cachedEntity := &CachedEntity{
			Entity: entity,
			Key:    entity.Key(),
		}

		jsonCachedEntity, err := json.Marshal(cachedEntity)
		if err != nil {
			panic(err)
		}

		batch.items = append(batch.items, &memcache.Item{
			Key:   cacheable.CacheID(),
			Value: jsonCachedEntity,
		})
	}
}
