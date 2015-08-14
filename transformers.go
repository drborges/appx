package appx

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
	"encoding/json"
	"github.com/drborges/riversv2/rx"
)

type transformersBuilder struct {
	context rx.Context
}

func NewTransformer(context rx.Context) *transformersBuilder {
	return &transformersBuilder{context}
}

func (builder *transformersBuilder) CacheableWithNonEmptyCacheKey(data rx.T) bool {
	cacheable, ok := data.(Cacheable)
	return ok && cacheable.CacheID() != ""
}

func (builder *transformersBuilder) ResolvedKeys(data rx.T) bool {
	entity, _ := data.(Entity)
	return entity.HasKey()
}

func (builder *transformersBuilder) EntitiesWithNonEmptyCacheIDs(data rx.T) bool {
	cacheable, _ := data.(Cacheable)
	return cacheable.CacheID() != ""
}

func (builder *transformersBuilder) ResolveEntityKeySilently(context appengine.Context) rx.MapFn {
	return func(data rx.T) rx.T {
		entity := data.(Entity)
		NewKeyResolver(context).Resolve(entity)
		return entity
	}
}

func (builder *transformersBuilder) ResolveEntityKey(context appengine.Context) rx.MapFn {
	return func(data rx.T) rx.T {
		entity := data.(Entity)
		if err := NewKeyResolver(context).Resolve(entity); err != nil {
			panic(err)
		}
		return entity
	}
}

func (builder *transformersBuilder) MemcacheLoadBatchOf(size int) rx.Batch {
	return &BatchCacheLoader{
		Size:  size,
		Items: make(map[string]*CachedEntity),
	}
}

func (builder *transformersBuilder) MemcacheSaveBatchOf(size int) rx.Batch {
	return &BatchCacheSetter{
		Size:  size,
		Items: []*memcache.Item{},
	}
}

func (builder *transformersBuilder) MemcacheDeleteBatchOf(size int) rx.Batch {
	return &BatchCacheDeleter{
		Size: size,
	}
}

func (builder *transformersBuilder) DatastoreBatchOf(size int) rx.Batch {
	return &BatchDatastore{Size: size}
}

func (builder *transformersBuilder) SaveMemcacheBatch(context appengine.Context) rx.MapFn {
	return func(data rx.T) rx.T {
		batch := data.(*BatchCacheSetter)
		if err := memcache.SetMulti(context, batch.Items); err != nil {
			panic(err)
		}
		return batch
	}
}

func (builder *transformersBuilder) SaveDatastoreBatch(context appengine.Context) rx.MapFn {
	return func(data rx.T) rx.T {
		batch := data.(*BatchDatastore)
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

		return batch
	}
}

func (builder *transformersBuilder) DeleteBatchFromCache(context appengine.Context) rx.MapFn {
	return func(data rx.T) rx.T {
		batch := data.(*BatchCacheDeleter)
		memcache.DeleteMulti(context, batch.Keys)
		return batch
	}
}

func (builder *transformersBuilder) DeleteBatchFromDatastore(context appengine.Context) rx.MapFn {
	return func(data rx.T) rx.T {
		batch := data.(*BatchDatastore)
		if err := datastore.DeleteMulti(context, batch.Keys); err != nil {
			panic(err)
		}
		return batch
	}
}

func (builder *transformersBuilder) LoadBatchFromCache(context appengine.Context) *observer {
	return &observer{
		context:    builder.context,
		onComplete: func(out rx.OutStream) {},
		onData: func(data rx.T, out rx.OutStream) {
			batch := data.(*BatchCacheLoader)
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
		},
	}
}

func (builder *transformersBuilder) LoadBatchFromDatastore(context appengine.Context) *observer {
	return &observer{
		context:    builder.context,
		onComplete: func(out rx.OutStream) {},
		onData: func(data rx.T, out rx.OutStream) {
			batch := data.(*BatchDatastore)
			if err := datastore.GetMulti(context, batch.Keys, batch.Items); err != nil {
				panic(err)
			}
		},
	}
}

func (builder *transformersBuilder) QueryEntityFromDatastore(context appengine.Context) *transformer {
	return &transformer{
		context: builder.context,
		transform: func(data rx.T) bool {
			entity, ok := data.(Entity)
			if !ok {
				return false
			}

			queryable, ok := data.(Queryable)
			if !ok {
				return true
			}

			key, err := queryable.Query().Run(context).Next(data)
			if err != nil {
				panic(err)
			}

			entity.SetKey(key)
			return false
		},
	}
}
