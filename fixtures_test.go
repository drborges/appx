package appx_test

import (
	"appengine"
	"appengine/datastore"
	"encoding/json"
	"github.com/drborges/appx"
	"time"
)

func toJSON(e appx.Entity) []byte {
	cachedEntity := &appx.CachedEntity{
		Entity: e,
		Key:    e.Key(),
	}
	json, _ := json.Marshal(cachedEntity)
	return json
}

func createAll(c appengine.Context, tags ...*Tag) {
	keys := make([]*datastore.Key, len(tags))
	for i, tag := range tags {
		appx.NewKeyResolver(c).Resolve(tag)
		keys[i] = tag.Key()
	}
	datastore.PutMulti(c, keys, tags)
	time.Sleep(2 * time.Second)
}
