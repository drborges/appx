package appx

import (
	"appengine/memcache"
	"encoding/json"
	"github.com/drborges/riversv2/rx"
)

type BatchCacheSetter struct {
	Size  int
	Items []*memcache.Item
}

func (batch *BatchCacheSetter) Full() bool {
	return len(batch.Items) == batch.Size
}

func (batch *BatchCacheSetter) Empty() bool {
	return len(batch.Items) == 0
}

func (batch *BatchCacheSetter) Add(data rx.T) {
	entity := data.(Entity)
	cacheable := data.(Cacheable)

	cachedEntity := &CachedEntity{
		Entity: entity,
		Key:    entity.Key(),
	}

	jsonCachedEntity, err := json.Marshal(cachedEntity)
	if err != nil {
		panic(err)
	}

	batch.Items = append(batch.Items, &memcache.Item{
		Key:   cacheable.CacheID(),
		Value: jsonCachedEntity,
	})
}

func (batch *BatchCacheSetter) Commit(out rx.OutStream) {
	out <- &BatchCacheSetter{
		Size:  batch.Size,
		Items: batch.Items,
	}

	batch.Items = []*memcache.Item{}
}
