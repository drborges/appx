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
		transformer := appx.NewTransformer(riversCtx).ResolveEntityKey2(gaeCtx)

		Convey("When I transform entities with valid key spec from the inbound stream", func() {
			in, out := rx.NewStream(3)
			out <- entityWithIntID
			out <- entityWithIncompleteKey
			out <- entityWithStringID
			close(out)

			stream := transformer.Transform(in)

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

		Convey("When I transform entities with keys already resolved", func() {
			key1 := datastore.NewKey(gaeCtx, "entity", "123", 0, nil)
			key2 := datastore.NewKey(gaeCtx, "entity", "", 123, nil)
			entityWithIncompleteKey.SetKey(key1)
			entityWithStringID.SetKey(key2)

			in, out := rx.NewStream(3)
			out <- entityWithIncompleteKey
			out <- entityWithStringID
			close(out)

			stream := transformer.Transform(in)

			Convey("Then all entities are sent downstream", func() {
				So(stream.Read(), ShouldResemble, []rx.T{
					entityWithIncompleteKey,
					entityWithStringID,
				})

				Convey("And entities keys are not replaced", func() {
					So(entityWithIncompleteKey.Key(), ShouldResemble, key1)
					So(entityWithStringID.Key(), ShouldResemble, key2)
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
		SSN:   "123123123",
		keySpec: &appx.KeySpec{
			Kind:     "Users",
			StringID: "borges",
		},
	}

	parentKey := datastore.NewKey(gaeCtx, "Parent", "parent id", 0, nil)
	user.SetParentKey(parentKey)

	Convey("Given I have a query entity from datastore transformer", t, func() {
		riversCtx := rivers.NewContext()
		transformer := appx.NewTransformer(riversCtx).QueryEntityFromDatastore(gaeCtx)

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

			stream := transformer.Transform(in)

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

				stream := transformer.Transform(in)

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

func TestUpdateEntitiesInDatastore(t *testing.T) {
	gaeCtx, _ := aetest.NewContext(nil)
	defer gaeCtx.Close()

	userMissingKey := NewUser(User{
		Name:  "Another User",
		Email: "user@email.com",
	})

	newUser := NewUser(User{
		Name:  "Diego",
		Email: "diego@email.com",
	})

	existentUser := NewUser(User{
		Name:  "Borges",
		Email: "borges@email.com",
	})

	appx.NewKeyResolver(gaeCtx).Resolve(newUser)
	appx.NewKeyResolver(gaeCtx).Resolve(existentUser)

	newUserKeyBeforeUpdate := newUser.Key()

	Convey("Given I have a update entities in datastore transformer", t, func() {
		riversCtx := rivers.NewContext()
		transformer := appx.NewTransformer(riversCtx).UpdateEntitiesInDatastore(gaeCtx)

		Convey("And I have an existent entity", func() {
			datastore.Put(gaeCtx, existentUser.Key(), existentUser)

			Convey("When I transform the inbound stream", func() {
				existentUser.Name = "borges2"

				in, out := rx.NewStream(4)
				out <- existentUser
				out <- userMissingKey
				out <- newUser
				out <- "notAnEntity"
				close(out)

				stream := transformer.Transform(in)

				Convey("Then all entities are sent downstream", func() {
					So(stream.Read(), ShouldResemble, []rx.T{
						userMissingKey,
						"notAnEntity",
						existentUser,
						newUser,
					})

					Convey("And new users are created", func() {
						var userFromDatastore User
						datastore.Get(gaeCtx, newUser.Key(), &userFromDatastore)

						So(newUser.Key(), ShouldNotEqual, newUserKeyBeforeUpdate)
						So(userFromDatastore.Name, ShouldResemble, newUser.Name)
						So(userFromDatastore.Email, ShouldResemble, newUser.Email)

						Convey("And existent users are updated", func() {
							var userFromDatastore User
							datastore.Get(gaeCtx, existentUser.Key(), &userFromDatastore)

							So(userFromDatastore.Name, ShouldResemble, existentUser.Name)
							So(userFromDatastore.Email, ShouldResemble, existentUser.Email)
						})
					})
				})
			})
		})
	})
}

func TestUpdateEntitiesInCache(t *testing.T) {
	gaeCtx, _ := aetest.NewContext(nil)
	defer gaeCtx.Close()

	cachedUser := NewUserWithParent(User{
		Name:  "Borges",
		Email: "drborges.cic@gmail.com",
		SSN:   "123123123",
	})

	newUser := NewUserWithParent(User{
		Name:  "Diego",
		Email: "diego@email.com",
		SSN:   "321321321",
	})

	cachedUser.SetParentKey(datastore.NewKey(gaeCtx, "Parent", "parent id", 0, nil))
	newUser.SetParentKey(datastore.NewKey(gaeCtx, "Parent", "parent id", 0, nil))
	appx.NewKeyResolver(gaeCtx).Resolve(cachedUser)
	appx.NewKeyResolver(gaeCtx).Resolve(newUser)

	Convey("Given I have a update entities in cache transformer", t, func() {
		riversCtx := rivers.NewContext()
		transformer := appx.NewTransformer(riversCtx).UpdateEntitiesInCache(gaeCtx)

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
				cachedUser.Name = "borges"

				in, out := rx.NewStream(4)
				out <- cachedUser
				out <- notCacheable
				out <- newUser
				out <- "notAnEntity"
				close(out)

				stream := transformer.Transform(in)

				Convey("Then data that is not cacheable and nor an entity are sent downstream", func() {
					So(<-stream, ShouldEqual, notCacheable)
					So(<-stream, ShouldEqual, "notAnEntity")

					_, opened := <-stream
					So(opened, ShouldBeFalse)

					Convey("And all cacheable entities are saved in the cache", func() {
						cachedUserFromCache := &User{}
						newUserFromCache := &User{}

						cachedEntity1 := &appx.CachedEntity{
							Entity: cachedUserFromCache,
						}

						cachedEntity2 := &appx.CachedEntity{
							Entity: newUserFromCache,
						}

						_, err := memcache.JSON.Get(gaeCtx, cachedUser.CacheID(), cachedEntity1)
						So(err, ShouldBeNil)

						_, err = memcache.JSON.Get(gaeCtx, newUser.CacheID(), cachedEntity2)
						So(err, ShouldBeNil)

						cachedEntity1.Entity.SetKey(cachedEntity1.Key)
						cachedEntity2.Entity.SetKey(cachedEntity2.Key)

						So(cachedUserFromCache.Name, ShouldResemble, cachedUser.Name)
						So(cachedUserFromCache.Email, ShouldResemble, cachedUser.Email)
						So(cachedUserFromCache.Key(), ShouldResemble, cachedUser.Key())
						So(cachedUserFromCache.ParentKey(), ShouldResemble, cachedUser.ParentKey())

						So(newUserFromCache.Name, ShouldResemble, newUser.Name)
						So(newUserFromCache.Email, ShouldResemble, newUser.Email)
						So(newUserFromCache.Key(), ShouldResemble, newUser.Key())
						So(newUserFromCache.ParentKey(), ShouldResemble, newUser.ParentKey())
					})
				})
			})
		})
	})
}

func TestDeleteEntitiesFromCache(t *testing.T) {
	gaeCtx, _ := aetest.NewContext(nil)
	defer gaeCtx.Close()

	cachedUser1 := NewUserWithParent(User{
		Name:  "Borges",
		Email: "drborges.cic@gmail.com",
		SSN:   "123123123",
	})

	cachedUser2 := NewUserWithParent(User{
		Name:  "Borges",
		Email: "drborges.cic@gmail.com",
		SSN:   "123123123",
	})

	appx.NewKeyResolver(gaeCtx).Resolve(cachedUser1)
	appx.NewKeyResolver(gaeCtx).Resolve(cachedUser2)

	Convey("Given I have a load entity from cache transformer", t, func() {
		riversCtx := rivers.NewContext()
		transformer := appx.NewTransformer(riversCtx).DeleteEntitiesFromCache(gaeCtx)

		Convey("And I have a cached entity", func() {
			cached1 := appx.CachedEntity{
				Entity:    cachedUser1,
				Key:       cachedUser1.Key(),
				ParentKey: cachedUser1.ParentKey(),
			}

			cached2 := appx.CachedEntity{
				Entity:    cachedUser2,
				Key:       cachedUser2.Key(),
				ParentKey: cachedUser2.ParentKey(),
			}

			memcache.JSON.Set(gaeCtx, &memcache.Item{
				Key:    cachedUser1.CacheID(),
				Object: cached1,
			})

			memcache.JSON.Set(gaeCtx, &memcache.Item{
				Key:    cachedUser2.CacheID(),
				Object: cached2,
			})

			Convey("When I transform the inbound entity stream", func() {
				notCacheable := &Entity{}
				userNotCached := NewUser(User{})
				userFromCache1 := NewUser(User{
					SSN: cachedUser1.SSN,
				})

				userFromCache2 := NewUser(User{
					SSN: cachedUser2.SSN,
				})

				in, out := rx.NewStream(4)
				out <- userFromCache1
				out <- userNotCached
				out <- userFromCache2
				out <- notCacheable
				close(out)

				stream := transformer.Transform(in)

				Convey("Then all entities are sent downstream", func() {
					So(<-stream, ShouldEqual, userFromCache1)
					So(<-stream, ShouldEqual, userNotCached)
					So(<-stream, ShouldEqual, userFromCache2)
					So(<-stream, ShouldEqual, notCacheable)

					_, opened := <-stream
					So(opened, ShouldBeFalse)

					Convey("And all cached entities are deleted", func() {
						_, err := memcache.Get(gaeCtx, cachedUser1.CacheID())
						So(err, ShouldEqual, memcache.ErrCacheMiss)
						_, err = memcache.Get(gaeCtx, cachedUser2.CacheID())
						So(err, ShouldEqual, memcache.ErrCacheMiss)
					})
				})
			})
		})
	})
}

func TestDeleteEntitiesFromDatastore(t *testing.T) {
	gaeCtx, _ := aetest.NewContext(nil)
	defer gaeCtx.Close()

	userMissingKey := NewUser(User{
		Name:  "Another User",
		Email: "user@email.com",
	})

	existentUser1 := NewUser(User{
		Name:  "Diego",
		Email: "diego@email.com",
	})

	existentUser2 := NewUser(User{
		Name:  "Borges",
		Email: "borges@email.com",
	})

	appx.NewKeyResolver(gaeCtx).Resolve(existentUser1)
	appx.NewKeyResolver(gaeCtx).Resolve(existentUser2)

	Convey("Given I have a delete entities from datastore transformer", t, func() {
		riversCtx := rivers.NewContext()
		transformer := appx.NewTransformer(riversCtx).DeleteEntitiesFromDatastore(gaeCtx)

		Convey("And I have existent entities", func() {
			datastore.Put(gaeCtx, existentUser1.Key(), existentUser1)
			datastore.Put(gaeCtx, existentUser2.Key(), existentUser2)

			Convey("When I transform the inbound stream", func() {
				in, out := rx.NewStream(4)
				out <- existentUser2
				out <- userMissingKey
				out <- existentUser1
				out <- "notAnEntity"
				close(out)

				stream := transformer.Transform(in)

				Convey("Then entities missing keys and non entities are sent downstream", func() {
					So(stream.Read(), ShouldResemble, []rx.T{userMissingKey, "notAnEntity"})

					Convey("And entities with keys are deleted from datastore", func() {
						err := datastore.Get(gaeCtx, existentUser1.Key(), existentUser1)
						So(err, ShouldEqual, datastore.ErrNoSuchEntity)

						err = datastore.Get(gaeCtx, existentUser2.Key(), existentUser2)
						So(err, ShouldEqual, datastore.ErrNoSuchEntity)
					})
				})
			})
		})
	})
}
