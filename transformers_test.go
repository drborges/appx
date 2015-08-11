package appx_test

import (
	"appengine/aetest"
	"appengine/datastore"
	"appengine/memcache"
	"github.com/drborges/appxv2"
	"github.com/drborges/riversv2"
	"github.com/drborges/riversv2/rx"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestResolveEntityKey(t *testing.T) {
	gaeCtx, _ := aetest.NewContext(nil)
	defer gaeCtx.Close()

	entityWithIntID := &Entity{
		keySpec: &appx.KeySpec{
			Kind:  "Entity",
			IntID: 123,
		},
	}

	entityWithIncompleteKey := &Entity{
		keySpec: &appx.KeySpec{
			Kind: "Entity",
		},
	}

	entityWithStringID := &Entity{
		keySpec: &appx.KeySpec{
			Kind:     "Entity",
			StringID: "321",
		},
	}

	Convey("Given I have a resolve entity key transformer", t, func() {
		riversCtx := rivers.NewContext()
		resolver := appx.NewTransformer(riversCtx).ResolveEntityKey(gaeCtx)

		Convey("When I transform entities with valid key spec from the inbound stream", func() {
			in, out := rx.NewStream(3)
			out <- entityWithIntID
			out <- entityWithIncompleteKey
			out <- entityWithStringID
			close(out)

			stream := resolver.Transform(in)

			Convey("Then all entities are sent downstream", func() {
				So(stream.Read(), ShouldResemble, []rx.T{
					entityWithIntID,
					entityWithIncompleteKey,
					entityWithStringID,
				})

				Convey("And entities with complete key specs have their keys resolved", func() {
					So(entityWithIntID.Key(), ShouldNotBeNil)
					So(entityWithStringID.Key(), ShouldNotBeNil)
					So(entityWithIncompleteKey.Key(), ShouldBeNil)
				})
			})
		})
	})
}

func TestLoadEntityFromCache(t *testing.T) {
	gaeCtx, _ := aetest.NewContext(nil)
	defer gaeCtx.Close()

	cachedUser := NewUserWithParent(User{
		Name:  "Borges",
		Email: "drborges.cic@gmail.com",
	})

	cachedUser.SetParentKey(datastore.NewKey(gaeCtx, "Parent", "parent id", 0, nil))
	appx.NewKeyResolver(gaeCtx).Resolve(cachedUser)

	Convey("Given I have a load entity from cache transformer", t, func() {
		riversCtx := rivers.NewContext()
		loader := appx.NewTransformer(riversCtx).LoadEntitiesFromCacheInBatch(gaeCtx)

		Convey("And I have a cached entity", func() {
			cached := appx.CachedEntity{
				Entity:    cachedUser,
				Key:       cachedUser.Key(),
				ParentKey: cachedUser.ParentKey(),
			}

			memcache.JSON.Set(gaeCtx, &memcache.Item{
				Key:    cachedUser.CacheID(),
				Object: cached,
			})

			Convey("When I transform the inbound entity stream", func() {
				notCacheable := &Entity{}
				userNotCached := NewUser(User{})
				userFromCache := NewUser(User{
					Email: cachedUser.Email,
				})

				in, out := rx.NewStream(3)
				out <- userFromCache
				out <- userNotCached
				out <- notCacheable
				close(out)

				stream := loader.Transform(in)

				Convey("Then non cached entities are sent downstream", func() {
					So(<-stream, ShouldEqual, userNotCached)
					So(<-stream, ShouldEqual, notCacheable)

					_, opened := <-stream
					So(opened, ShouldBeFalse)

					Convey("And all cached entities are loaded", func() {
						So(userFromCache.Name, ShouldEqual, cachedUser.Name)
						So(userFromCache.Email, ShouldEqual, cachedUser.Email)
						So(userFromCache.Key(), ShouldResemble, cachedUser.Key())
						So(userFromCache.ParentKey(), ShouldResemble, cachedUser.ParentKey())
					})
				})
			})
		})
	})
}

func TestLookupEntityFromDatastore(t *testing.T) {
	gaeCtx, _ := aetest.NewContext(nil)
	defer gaeCtx.Close()

	user := NewUserWithParent(User{
		Name:  "Borges",
		Email: "drborges.cic@gmail.com",
	})

	parentKey := datastore.NewKey(gaeCtx, "Parent", "parent id", 0, nil)
	user.SetParentKey(parentKey)

	Convey("Given I have a lookup entity from datastore transformer", t, func() {
		riversCtx := rivers.NewContext()
		lookup := appx.NewTransformer(riversCtx).LookupEntitiesFromDatastoreInBatch(gaeCtx)

		Convey("When I transform the inbound stream with entities that cannot be looked up", func() {
			userMissingKey := &User{}
			nonExistentUser := NewUser(User{Name: "Borges"})
			appx.NewKeyResolver(gaeCtx).Resolve(nonExistentUser)

			in, out := rx.NewStream(2)
			out <- nonExistentUser
			out <- userMissingKey
			close(out)

			stream := lookup.Transform(in)

			Convey("Then no entities are sent downstream ", func() {
				So(stream.Read(), ShouldResemble, []rx.T{userMissingKey})

				Convey("And context is closed with error", func() {
					_, opened := <-riversCtx.Closed()
					So(opened, ShouldBeFalse)
					So(riversCtx.Err().Error(), ShouldResemble, datastore.ErrNoSuchEntity.Error())
				})
			})
		})

		Convey("And I have an entity in datastore", func() {
			err := appx.NewKeyResolver(gaeCtx).Resolve(user)
			So(err, ShouldBeNil)

			_, err = datastore.Put(gaeCtx, user.Key(), user)
			So(err, ShouldBeNil)

			Convey("When I transform the inbound entity stream", func() {
				userFromDatastore := NewUserWithParent(User{Name: "Borges"})
				userFromDatastore.SetParentKey(parentKey)
				appx.NewKeyResolver(gaeCtx).Resolve(userFromDatastore)

				userMissingKey := NewUser(User{})
				userWithIncompleteKey := NewUser(User{})
				userWithIncompleteKey.SetKey(datastore.NewIncompleteKey(gaeCtx, "Users", nil))

				in, out := rx.NewStream(3)
				out <- userFromDatastore
				out <- userMissingKey
				out <- userWithIncompleteKey
				close(out)

				stream := lookup.Transform(in)

				Convey("Then non existent entities are sent downstream", func() {
					So(<-stream, ShouldResemble, userMissingKey)
					So(<-stream, ShouldResemble, userWithIncompleteKey)

					_, opened := <-stream
					So(opened, ShouldBeFalse)

					Convey("And all existent entities are loaded", func() {
						So(userFromDatastore.Name, ShouldEqual, user.Name)
						So(userFromDatastore.Email, ShouldEqual, user.Email)
						So(userFromDatastore.Key(), ShouldResemble, user.Key())
						So(userFromDatastore.ParentKey(), ShouldResemble, user.ParentKey())
					})
				})
			})
		})
	})
}

func TestQueryEntityFromDatastore(t *testing.T) {
	gaeCtx, _ := aetest.NewContext(nil)
	defer gaeCtx.Close()

	user := &User{
		Name:  "Borges",
		Email: "diego@email.com",
		keySpec: &appx.KeySpec{
			Kind:     "Users",
			StringID: "borges",
		},
	}

	parentKey := datastore.NewKey(gaeCtx, "Parent", "parent id", 0, nil)
	user.SetParentKey(parentKey)

	Convey("Given I have a query entity from datastore transformer", t, func() {
		riversCtx := rivers.NewContext()
		querier := appx.NewTransformer(riversCtx).QueryEntityFromDatastore(gaeCtx)

		Convey("When I transform the inbound stream with non existent entity", func() {
			userFromDatastore := &User{
				Email: "diego@email.com",
				keySpec: &appx.KeySpec{
					Kind: "Users",
				},
			}

			in, out := rx.NewStream(2)
			out <- userFromDatastore
			close(out)

			stream := querier.Transform(in)

			Convey("Then no entities are sent downstream", func() {
				So(stream.Read(), ShouldBeEmpty)

				Convey("And context is closed with error", func() {
					_, opened := <-riversCtx.Closed()
					So(opened, ShouldBeFalse)
					So(riversCtx.Err(), ShouldNotBeNil)
				})
			})
		})

		Convey("And I have an entity in datastore", func() {
			err := appx.NewKeyResolver(gaeCtx).Resolve(user)
			So(err, ShouldBeNil)

			_, err = datastore.Put(gaeCtx, user.Key(), user)
			So(err, ShouldBeNil)

			// Give datastore some time so that the created entity is available to be queried
			time.Sleep(200 * time.Millisecond)

			Convey("When I transform the inbound stream", func() {
				userFromDatastore := &User{
					Email: "diego@email.com",
					keySpec: &appx.KeySpec{
						Kind: "Users",
					},
				}

				in, out := rx.NewStream(2)
				out <- userFromDatastore
				close(out)

				stream := querier.Transform(in)

				Convey("Then no entities are sent downstream", func() {
					So(stream.Read(), ShouldBeEmpty)

					Convey("And queryable entities are loaded from datastore", func() {
						So(userFromDatastore.Name, ShouldEqual, user.Name)
						So(userFromDatastore.Email, ShouldEqual, user.Email)
						So(userFromDatastore.Key(), ShouldResemble, user.Key())
						So(userFromDatastore.ParentKey(), ShouldResemble, user.ParentKey())
					})
				})
			})
		})
	})
}
