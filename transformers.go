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
		riversCtx: builder.context,
		gaeCtx:    context,
		transform: func(e Entity) bool {
			NewKeyResolver(context).Resolve(e)
			return true
		},
	}
}

func (builder *transformersBuilder) LoadEntityFromCache(context appengine.Context) *transformer {
	return &transformer{
		riversCtx: builder.context,
		gaeCtx:    context,
		transform: func(e Entity) bool {
			if cacheable, ok := e.(Cacheable); ok {
				cachedEntity := CachedEntity{
					Entity: e,
				}
				_, err := memcache.JSON.Get(context, cacheable.CacheID(), &cachedEntity)
				if err == memcache.ErrCacheMiss {
					return true
				}

				if err != nil {
					panic(err)
				}

				e.SetKey(cachedEntity.Key)
				return false
			}

			return true
		},
	}
}

func (builder *transformersBuilder) LookupEntityFromDatastore(context appengine.Context) *transformer {
	return &transformer{
		riversCtx: builder.context,
		gaeCtx:    context,
		transform: func(e Entity) bool {
			// Send entity to the next downstream transformer in
			// case it is not possible to look it up from datastore
			if !e.HasKey() || e.Key().Incomplete() {
				return true
			}

			if err := datastore.Get(context, e.Key(), e); err != nil {
				panic(err)
			}

			return false
		},
	}
}

func (builder *transformersBuilder) QueryEntityFromDatastore(context appengine.Context) *transformer {
	return &transformer{
		riversCtx: builder.context,
		gaeCtx:    context,
		transform: func(e Entity) bool {
			// Send entity to the next downstream transformer in
			// case it is not possible to look it up from datastore

			if queryable, ok := e.(Queryable); ok {
				key, err := queryable.Query().Run(context).Next(e)
				if err != nil {
					panic(err)
				}

				e.SetKey(key)
				return false
			}

			return true
		},
	}
}
