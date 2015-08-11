package appx

import (
	"appengine"
	"appengine/memcache"
	"encoding/json"
	"github.com/drborges/riversv2/rx"
)

type cacheBatchSetter struct {
	context appengine.Context
	size    int
	items   []*memcache.Item
}

func (batch *cacheBatchSetter) Full() bool {
	return len(batch.items) == batch.size
}

func (batch *cacheBatchSetter) Empty() bool {
	return len(batch.items) == 0
}

func (batch *cacheBatchSetter) Commit(out rx.OutStream) {
	if batch.Empty() {
		return
	}

	if err := memcache.SetMulti(batch.context, batch.items); err != nil {
		panic(err)
	}

	batch.items = []*memcache.Item{}
}

func (batch *cacheBatchSetter) Add(data rx.T) {
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
