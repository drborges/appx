package appx

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
	"encoding/json"
	"github.com/drborges/rivers/rx"
)

type stepsBuilder struct {
	context rx.Context
}

func NewStep(context rx.Context) *stepsBuilder {
	return &stepsBuilder{context}
}

// TODO write test case
func (builder *stepsBuilder) CacheableEntitiesWithCacheKey(data rx.T) bool {
	cacheable, ok := data.(Cacheable)
	return ok && cacheable.CacheID() != ""
}

// TODO write test case
func (builder *stepsBuilder) ResolvedKeys(data rx.T) bool {
	entity, _ := data.(Entity)
	return entity.HasKey()
}

func (builder *stepsBuilder) EntitiesWithNonEmptyCacheIDs(data rx.T) bool {
	cacheable, _ := data.(Cacheable)
	return cacheable.CacheID() != ""
}

func (builder *stepsBuilder) ResolveEntityKeySilently(context appengine.Context) rx.EachFn {
	return func(data rx.T) {
		NewKeyResolver(context).Resolve(data.(Entity))
	}
}

func (builder *stepsBuilder) ResolveEntityKey(context appengine.Context) rx.EachFn {
	return func(data rx.T) {
		if err := NewKeyResolver(context).Resolve(data.(Entity)); err != nil {
			panic(err)
		}
	}
}

func (builder *stepsBuilder) MemcacheLoadBatchOf(size int) rx.Batch {
	return &MemcacheLoadBatch{
		Size:  size,
		Items: make(map[string]*CachedEntity),
	}
}

func (builder *stepsBuilder) MemcacheSaveBatchOf(size int) rx.Batch {
	return &MemcacheSaveBatch{
		Size:  size,
		Items: []*memcache.Item{},
	}
}

func (builder *stepsBuilder) MemcacheDeleteBatchOf(size int) rx.Batch {
	return &MemcacheDeleteBatch{
		Size: size,
	}
}

func (builder *stepsBuilder) DatastoreBatchOf(size int) rx.Batch {
	return &DatastoreBatch{Size: size}
}

func (builder *stepsBuilder) SaveMemcacheBatch(context appengine.Context) rx.EachFn {
	return func(data rx.T) {
		if err := memcache.SetMulti(context, data.(*MemcacheSaveBatch).Items); err != nil {
			panic(err)
		}
	}
}

// TODO write test case to make sure keys are set back to the entities after saving batch
func (builder *stepsBuilder) SaveDatastoreBatch(context appengine.Context) rx.EachFn {
	return func(data rx.T) {
		batch := data.(*DatastoreBatch)
		keys, err := datastore.PutMulti(context, batch.Keys, batch.Items)

		if err != nil {
			panic(err)
		}

		// Set refreshed keys back to the entities
		// For new entities with incomplete keys, the actual
		// key is the one returned by PutMulti
		for i, key := range keys {
			batch.Items[i].SetKey(key)
		}
	}
}

func (builder *stepsBuilder) DeleteBatchFromCache(context appengine.Context) rx.EachFn {
	return func(data rx.T) {
		memcache.DeleteMulti(context, data.(*MemcacheDeleteBatch).Keys)
	}
}

func (builder *stepsBuilder) DeleteBatchFromDatastore(context appengine.Context) rx.EachFn {
	return func(data rx.T) {
		if err := datastore.DeleteMulti(context, data.(*DatastoreBatch).Keys); err != nil {
			panic(err)
		}
	}
}

func (builder *stepsBuilder) LoadBatchFromCache(context appengine.Context) rx.OnDataFn {
	return func(data rx.T, out rx.OutStream) {
		batch := data.(*MemcacheLoadBatch)
		items, err := memcache.GetMulti(context, batch.Keys)

		if err != nil {
			panic(err)
		}

		for id, item := range items {
			if err := json.Unmarshal(item.Value, batch.Items[id]); err != nil {
				panic(err)
			}
			// Set entity key back
			batch.Items[id].Entity.SetKey(batch.Items[id].Key)
			delete(batch.Items, id)
		}

		// In case of cache misses, send entities
		// downstream to be handled by the next transformer
		if !batch.Empty() {
			for _, item := range batch.Items {
				out <- item.Entity
			}
		}
	}
}

func (builder *stepsBuilder) LoadBatchFromDatastore(context appengine.Context) rx.EachFn {
	return func(data rx.T) {
		batch := data.(*DatastoreBatch)
		if err := datastore.GetMulti(context, batch.Keys, batch.Items); err != nil {
			panic(err)
		}
	}
}

func (builder *stepsBuilder) QueryEntityFromDatastore(context appengine.Context) rx.EachFn {
	return func(data rx.T) {
		if err := NewItemsIterator(context, data.(Queryable).Query()).LoadNext(data); err != nil {
			panic(err)
		}
	}
}
