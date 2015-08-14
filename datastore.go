package appx

import (
	"appengine"
	"appengine/datastore"
	"github.com/drborges/rivers"
)

type Datastore struct {
	context appengine.Context
}

func NewDatastore(context appengine.Context) *Datastore {
	return &Datastore{context}
}

func (datastore *Datastore) Load(entities ...Entity) error {
	context := rivers.NewContext()
	step := NewStep(context)
	pipeline := rivers.NewWith(context)

	cacheableEntities, nonCacheableEntities := pipeline.FromSlice(entities).
		Each(step.ResolveEntityKeySilently(datastore.context)).
		Partition(step.CacheableEntitiesWithCacheKey)

	cacheMisses, cacheMissesToBeCached := cacheableEntities.
		BatchBy(step.MemcacheLoadBatchOf(1000)).
		ProcessWith(step.LoadBatchFromCache(datastore.context)).
		Split()

	entitiesWithKeys, entitiesMissingKeys := pipeline.Combine(
		nonCacheableEntities.Sink(),
		cacheMisses.Sink()).
		Partition(step.ResolvedKeys)

	notLoadedEntities := entitiesWithKeys.
		BatchBy(step.DatastoreBatchOf(1000)).
		Each(step.LoadBatchFromDatastore(datastore.context))

	notQueriedEntities := entitiesMissingKeys.
		Each(step.QueryEntityFromDatastore(datastore.context))

	pipeline.Combine(
		notLoadedEntities.Sink(),
		notQueriedEntities.Sink()).Drain()

	return cacheMissesToBeCached.
		Filter(step.EntitiesWithNonEmptyCacheIDs).
		BatchBy(step.MemcacheSaveBatchOf(1000)).
		Each(step.SaveMemcacheBatch(datastore.context)).
		Drain()
}

func (datastore *Datastore) Save(entities ...Entity) error {
	context := rivers.NewContext()
	step := NewStep(context)
	pipeline := rivers.NewWith(context).FromSlice(entities)

	entitiesToBeCached, entitiesToBeSavedInDatastore := pipeline.
		Each(step.ResolveEntityKey(datastore.context)).
		Split()

	cacheStream := entitiesToBeCached.
		Filter(step.CacheableEntitiesWithCacheKey).
		BatchBy(step.MemcacheSaveBatchOf(500)).
		Each(step.SaveMemcacheBatch(datastore.context))

	datastoreStream := entitiesToBeSavedInDatastore.
		BatchBy(step.DatastoreBatchOf(500)).
		Each(step.SaveDatastoreBatch(datastore.context))

	return pipeline.Combine(
		cacheStream.Sink(),
		datastoreStream.Sink()).Drain()
}

func (datastore *Datastore) Delete(entities ...Entity) error {
	context := rivers.NewContext()
	step := NewStep(context)
	pipeline := rivers.NewWith(context).FromSlice(entities)

	deleteFromCache, deleteFromDatastore := pipeline.
		Each(step.ResolveEntityKey(datastore.context)).
		Split()

	cacheStream := deleteFromCache.
		Filter(step.CacheableEntitiesWithCacheKey).
		BatchBy(step.MemcacheDeleteBatchOf(500)).
		Each(step.DeleteBatchFromCache(datastore.context))

	datastoreStream := deleteFromDatastore.
		BatchBy(step.DatastoreBatchOf(500)).
		Each(step.DeleteBatchFromDatastore(datastore.context))

	return pipeline.Combine(
		cacheStream.Sink(),
		datastoreStream.Sink()).Drain()
}

func (datastore *Datastore) Query(q *datastore.Query) *runner {
	return &runner{datastore.context, q}
}
