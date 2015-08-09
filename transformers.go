package appx

import (
	"appengine"
	"github.com/drborges/riversv2/rx"
	"appengine/memcache"
)

type transformersBuilder struct {
	context rx.Context
}

func NewTransformer(context rx.Context) *transformersBuilder {
	return &transformersBuilder{context}
}

func (builder *transformersBuilder) ResolveEntityKey(gaeCtx appengine.Context) *transformer {
	return &transformer{
		riversCtx: builder.context,
		gaeCtx:    gaeCtx,
		transform: func(e Entity) bool {
			if err := NewKeyResolver(gaeCtx).Resolve(e); err != nil {
				panic(err)
			}

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
				cachedEntity := CachedEntity {
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
				e.SetParentKey(cachedEntity.ParentKey)
				return false
			}

			return true
		},
	}
}

