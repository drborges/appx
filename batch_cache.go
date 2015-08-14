package appx

import (
	"appengine/memcache"
	"encoding/json"
	"github.com/drborges/rivers/rx"
)

type MemcacheSaveBatch struct {
	Size  int
	Items []*memcache.Item
}

func (batch *MemcacheSaveBatch) Full() bool {
	return len(batch.Items) == batch.Size
}

func (batch *MemcacheSaveBatch) Empty() bool {
	return len(batch.Items) == 0
}

func (batch *MemcacheSaveBatch) Add(data rx.T) {
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

func (batch *MemcacheSaveBatch) Commit(out rx.OutStream) {
	out <- &MemcacheSaveBatch{
		Size:  batch.Size,
		Items: batch.Items,
	}

	batch.Items = []*memcache.Item{}
}

type MemcacheLoadBatch struct {
	Size  int
	Keys  []string
	Items map[string]*CachedEntity
}

func (batch *MemcacheLoadBatch) Full() bool {
	return len(batch.Keys) == batch.Size
}

func (batch *MemcacheLoadBatch) Empty() bool {
	return len(batch.Keys) == 0
}

func (batch *MemcacheLoadBatch) Add(data rx.T) {
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

func (batch *MemcacheLoadBatch) Commit(out rx.OutStream) {
	out <- &MemcacheLoadBatch{
		Size:  batch.Size,
		Keys:  batch.Keys,
		Items: batch.Items,
	}

	batch.Keys = []string{}
	batch.Items = make(map[string]*CachedEntity)
}

type MemcacheDeleteBatch struct {
	Size int
	Keys []string
}

func (batch *MemcacheDeleteBatch) Full() bool {
	return len(batch.Keys) == batch.Size
}

func (batch *MemcacheDeleteBatch) Empty() bool {
	return len(batch.Keys) == 0
}

func (batch *MemcacheDeleteBatch) Add(data rx.T) {
	cacheable := data.(Cacheable)
	batch.Keys = append(batch.Keys, cacheable.CacheID())
}

func (batch *MemcacheDeleteBatch) Commit(out rx.OutStream) {
	out <- &MemcacheDeleteBatch{
		Size: batch.Size,
		Keys: batch.Keys,
	}

	batch.Keys = []string{}
}
