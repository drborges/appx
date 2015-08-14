package appx

import (
	"appengine"
	"appengine/datastore"
	"github.com/drborges/riversv2"
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
		Map(step.ResolveEntityKeySilently(datastore.context)).
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
		ProcessWith(step.LoadBatchFromDatastore(datastore.context))

	notQueriedEntities := entitiesMissingKeys.
		ProcessWith(step.QueryEntityFromDatastore(datastore.context))

	pipeline.Combine(
		notLoadedEntities.Sink(),
		notQueriedEntities.Sink()).Drain()

	return cacheMissesToBeCached.
		Filter(step.EntitiesWithNonEmptyCacheIDs).
		BatchBy(step.MemcacheSaveBatchOf(1000)).
		Map(step.SaveMemcacheBatch(datastore.context)).
		Drain()
}

func (datastore *Datastore) Save(entities ...Entity) error {
	context := rivers.NewContext()
	step := NewStep(context)
	pipeline := rivers.NewWith(context).FromSlice(entities)

	entitiesToBeCached, entitiesToBeSavedInDatastore := pipeline.
		Map(step.ResolveEntityKey(datastore.context)).
		Split()

	cacheStream := entitiesToBeCached.
		Filter(step.CacheableEntitiesWithCacheKey).
		BatchBy(step.MemcacheSaveBatchOf(500)).
		Map(step.SaveMemcacheBatch(datastore.context))

	datastoreStream := entitiesToBeSavedInDatastore.
		BatchBy(step.DatastoreBatchOf(500)).
		Map(step.SaveDatastoreBatch(datastore.context))

	return pipeline.Combine(
		cacheStream.Sink(),
		datastoreStream.Sink()).Drain()
}

func (datastore *Datastore) Delete(entities ...Entity) error {
	context := rivers.NewContext()
	step := NewStep(context)
	pipeline := rivers.NewWith(context).FromSlice(entities)

	deleteFromCache, deleteFromDatastore := pipeline.
		Map(step.ResolveEntityKey(datastore.context)).
		Split()

	cacheStream := deleteFromCache.
		Filter(step.CacheableEntitiesWithCacheKey).
		BatchBy(step.MemcacheDeleteBatchOf(500)).
		ProcessWith(step.DeleteBatchFromCache(datastore.context))

	datastoreStream := deleteFromDatastore.
		BatchBy(step.DatastoreBatchOf(500)).
		ProcessWith(step.DeleteBatchFromDatastore(datastore.context))

	return pipeline.Combine(
		cacheStream.Sink(),
		datastoreStream.Sink()).Drain()
}

func (datastore *Datastore) Query(q *datastore.Query) *runner {
	return &runner{datastore.context, q}
}
