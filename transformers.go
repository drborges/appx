package appx

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
	"github.com/drborges/riversv2/rx"
)

type transformersBuilder struct {
	context rx.Context
}

func NewTransformer(context rx.Context) *transformersBuilder {
	return &transformersBuilder{context}
}

func (builder *transformersBuilder) ResolveEntityKey(context appengine.Context) *transformer {
	return &transformer{
		context: builder.context,
		transform: func(data rx.T) bool {
			entity, ok := data.(Entity)
			if !ok {
				return false
			}

			NewKeyResolver(context).Resolve(entity)
			return true
		},
	}
}

func (builder *transformersBuilder) LoadEntityFromCache(context appengine.Context) *transformer {
	return &transformer{
		context: builder.context,
		transform: func(data rx.T) bool {
			entity, ok := data.(Entity)
			if !ok {
				return false
			}

			cacheable, ok := data.(Cacheable)
			if !ok {
				return true
			}

			cachedEntity := CachedEntity{
				Entity: entity,
			}

			_, err := memcache.JSON.Get(context, cacheable.CacheID(), &cachedEntity)
			if err == memcache.ErrCacheMiss {
				return true
			}

			if err != nil {
				panic(err)
			}

			entity.SetKey(cachedEntity.Key)
			return false
		},
	}
}

func (builder *transformersBuilder) LookupEntityFromDatastore(context appengine.Context) *transformer {
	return &transformer{
		context: builder.context,
		transform: func(data rx.T) bool {
			entity, ok := data.(Entity)
			if !ok {
				return false
			}

			// Send entity to the next downstream transformer in
			// case it is not possible to look it up from datastore
			if !entity.HasKey() || entity.Key().Incomplete() {
				return true
			}

			// TODO Consider not panicing on this situation
			// In case the entity gets to this point with a key and still cannot
			// be lookuped up, should we move forward downstream?
			if err := datastore.Get(context, entity.Key(), entity); err != nil {
				panic(err)
			}

			return false
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
