package appx

import (
	"appengine"
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

			if entity.HasKey() {
				return true
			}

			NewKeyResolver(context).Resolve(entity)
			return true
		},
	}
}

func (builder *transformersBuilder) LoadEntitiesFromCache(context appengine.Context) *observer {
	batch := &cacheBatchLoader{
		context: context,
		size:    1000,
		items:   make(map[string]*CachedEntity),
	}

	return &observer{
		context: builder.context,

		onComplete: func(out rx.OutStream) {
			batch.Commit(out)
		},

		onData: func(data rx.T, out rx.OutStream) {
			entity, ok := data.(Entity)
			if !ok {
				out <- data
				return
			}

			cacheable, ok := data.(Cacheable)
			if !ok {
				out <- data
				return
			}

			if cacheable.CacheID() == "" {
				out <- entity
				return
			}

			batch.Add(data)
			if batch.Full() {
				batch.Commit(out)
			}
		},
	}
}

func (builder *transformersBuilder) LookupEntitiesFromDatastore(context appengine.Context) *observer {
	batch := &datastoreBatchLoader{}
	batch.context = context
	batch.size = 1000

	return &observer{
		context: builder.context,

		onComplete: func(out rx.OutStream) {
			batch.Commit(out)
		},

		onData: func(data rx.T, out rx.OutStream) {
			entity, ok := data.(Entity)
			if !ok {
				out <- data
				return
			}

			if !entity.HasKey() || entity.Key().Incomplete() {
				out <- data
				return
			}

			batch.Add(data)
			if batch.Full() {
				batch.Commit(out)
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

func (builder *transformersBuilder) UpdateEntitiesInDatastore(context appengine.Context) *observer {
	batch := &datastoreBatchSaver{}
	batch.context = context
	batch.size = 500

	return &observer{
		context: builder.context,

		onComplete: func(out rx.OutStream) {
			batch.Commit(out)
		},

		onData: func(data rx.T, out rx.OutStream) {
			entity, ok := data.(Entity)
			// TODO if it is not possible to update datastore,
			// Then the process should not go any further
			if !ok {
				out <- data
				return
			}

			if !entity.HasKey() {
				out <- data
				return
			}

			batch.Add(data)
			if batch.Full() {
				batch.Commit(out)
			}
		},
	}
}

func (builder *transformersBuilder) UpdateEntitiesInCache(context appengine.Context) *observer {
	batch := &cacheBatchSetter{
		context: context,
		size:    500,
		items:   []*memcache.Item{},
	}

	return &observer{
		context: builder.context,

		onComplete: func(out rx.OutStream) {
			batch.Commit(out)
		},

		onData: func(data rx.T, out rx.OutStream) {
			entity, ok := data.(Entity)
			if !ok {
				out <- data
				return
			}

			cacheable, ok := data.(Cacheable)
			if !ok {
				out <- data
				return
			}

			if cacheable.CacheID() == "" {
				out <- entity
				return
			}

			batch.Add(data)
			if batch.Full() {
				batch.Commit(out)
			}
		},
	}
}

func (builder *transformersBuilder) DeleteEntitiesFromCache(context appengine.Context) *observer {
	batch := &cacheBatchDeleter{
		context: context,
		size:    500,
	}

	return &observer{
		context: builder.context,

		onComplete: func(out rx.OutStream) {
			batch.Commit(out)
		},

		onData: func(data rx.T, out rx.OutStream) {
			entity, ok := data.(Entity)
			if !ok {
				out <- data
				return
			}

			cacheable, ok := data.(Cacheable)
			if !ok {
				out <- data
				return
			}

			if cacheable.CacheID() == "" {
				out <- entity
				return
			}

			batch.Add(data)
			if batch.Full() {
				batch.Commit(out)
			}

			out <- entity
		},
	}
}

func (builder *transformersBuilder) DeleteEntitiesFromDatastore(context appengine.Context) *observer {
	batch := &datastoreBatchDeleter{}
	batch.context = context
	batch.size = 500

	return &observer{
		context: builder.context,

		onComplete: func(out rx.OutStream) {
			batch.Commit(out)
		},

		onData: func(data rx.T, out rx.OutStream) {
			entity, ok := data.(Entity)
			if !ok {
				out <- data
				return
			}

			if !entity.HasKey() {
				out <- data
				return
			}

			batch.Add(data)
			if batch.Full() {
				batch.Commit(out)
			}
		},
	}
}
