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

type BatchCacheLoader struct {
	Size  int
	Keys []string
	Items map[string]*CachedEntity
}

func (batch *BatchCacheLoader) Full() bool {
	return len(batch.Keys) == batch.Size
}

func (batch *BatchCacheLoader) Empty() bool {
	return len(batch.Keys) == 0
}

func (batch *BatchCacheLoader) Add(data rx.T) {
	entity := data.(Entity)
	cacheable := data.(Cacheable)

	if batch.Items == nil {
		batch.Items = make(map[string]*CachedEntity)
	}

	batch.Keys = append(batch.Keys, cacheable.CacheID())
	batch.Items[cacheable.CacheID()] = &CachedEntity{
		Entity: entity,
	}
}

func (batch *BatchCacheLoader) Commit(out rx.OutStream) {
	out <- &BatchCacheLoader{
		Size: batch.Size,
		Keys: batch.Keys,
		Items: batch.Items,
	}

	batch.Keys = []string{}
	batch.Items = make(map[string]*CachedEntity)
}
