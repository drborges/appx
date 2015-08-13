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
	step := NewTransformer(context)
	pipeline := rivers.NewWith(context)

	cacheableEntities, nonCacheableEntities := pipeline.FromSlice(entities).
		Map(step.ResolveEntityKey(datastore.context)).
		Partition(step.CacheableWithNonEmptyCacheKey)

	cacheMisses, cacheMissesToBeCached := cacheableEntities.
		BatchBy(step.MemcacheLoadBatchOf(1000)).
		Apply(step.LoadBatchFromCache(datastore.context)).Split()

	entitiesWithKeys, entitiesMissingKeys := pipeline.
		Combine(nonCacheableEntities.Sink(), cacheMisses.Sink()).
		Partition(step.ResolvedKeys)

	notLoadedEntities := entitiesWithKeys.
		BatchBy(step.DatastoreBatchOf(1000)).
		Apply(step.LoadBatchFromDatastore(datastore.context))

	notQueriedEntities := entitiesMissingKeys.
		Apply(step.QueryEntityFromDatastore(datastore.context))

	pipeline.Combine(
		notLoadedEntities.Sink(),
		notQueriedEntities.Sink()).Drain()

	cacheMissesToBeCached.
		Filter(step.EntitiesWithNonEmptyCacheIDs).
		BatchBy(step.MemcacheSaveBatchOf(1000)).
		Apply(step.SaveMemcacheBatch(datastore.context)).
		Drain()

	return context.Err()
}

func (datastore *Datastore) Save(entities ...Entity) error {
	context := rivers.NewContext()
	transformer := NewTransformer(context)
	rivers.NewWith(context).FromSlice(entities).
		Apply(transformer.ResolveEntityKey2(datastore.context)).
		Apply(transformer.UpdateEntitiesInDatastore(datastore.context)).
		Apply(transformer.UpdateEntitiesInCache(datastore.context)).
		Drain()

	return context.Err()
}

func (datastore *Datastore) Delete(entities ...Entity) error {
	context := rivers.NewContext()
	transformer := NewTransformer(context)
	rivers.NewWith(context).FromSlice(entities).
		Apply(transformer.ResolveEntityKey2(datastore.context)).
		Apply(transformer.DeleteEntitiesFromCache(datastore.context)).
		Apply(transformer.DeleteEntitiesFromDatastore(datastore.context)).
		Drain()

	return context.Err()
}

func (datastore *Datastore) Query(q *datastore.Query) *runner {
	return &runner{datastore.context, q}
}
