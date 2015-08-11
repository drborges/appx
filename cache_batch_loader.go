package appx

import (
	"appengine"
	"appengine/memcache"
	"encoding/json"
	"github.com/drborges/riversv2/rx"
)

type cacheBatchLoader struct {
	context appengine.Context
	size    int
	ids     []string
	items   map[string]*CachedEntity
}

func (batch *cacheBatchLoader) Full() bool {
	return len(batch.items) == batch.size
}

func (batch *cacheBatchLoader) Empty() bool {
	return len(batch.items) == 0
}

func (batch *cacheBatchLoader) Commit(out rx.OutStream) {
	items, err := memcache.GetMulti(batch.context, batch.ids)

	if err != nil {
		panic(err)
	}

	for id, item := range items {
		if err := json.Unmarshal(item.Value, batch.items[id]); err != nil {
			panic(err)
		}
		// Set entity key back
		batch.items[id].Entity.SetKey(batch.items[id].Key)

		delete(batch.items, id)
	}

	// In case of cache misses, send entities
	// downstream to be handled by the next transformer
	batch.ids = []string{}
	if !batch.Empty() {
		for id, item := range batch.items {
			out <- item.Entity
			delete(batch.items, id)
		}
	}
}

func (batch *cacheBatchLoader) Add(data rx.T) {
	if cacheable, ok := data.(Cacheable); ok {
		entity := cacheable.(Entity)
		batch.ids = append(batch.ids, cacheable.CacheID())
		batch.items[cacheable.CacheID()] = &CachedEntity{
			Entity: entity,
		}
	}
}
