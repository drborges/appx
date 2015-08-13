package appx_test

import (
	"appengine/aetest"
	"appengine/memcache"
	"github.com/drborges/appxv2"
	"github.com/drborges/riversv2"
	"github.com/drborges/riversv2/rx"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestLoadBatchFromCache(t *testing.T) {
	gaeCtx, _ := aetest.NewContext(nil)
	defer gaeCtx.Close()

	Convey("Given I have a load batch from cache transformer", t, func() {
		riversCtx := rivers.NewContext()
		transformer := appx.NewTransformer(riversCtx).LoadBatchFromCache(gaeCtx)

		Convey("And I have a few entities in the cache", func() {
			user1 := NewUser(User{
				Name:  "Borges",
				Email: "borges@email.com",
				SSN:   "123123123",
			})

			user2 := NewUser(User{
				Name:  "Diego",
				Email: "diego@email.com",
				SSN:   "321321",
			})

			appx.NewKeyResolver(gaeCtx).Resolve(user1)
			appx.NewKeyResolver(gaeCtx).Resolve(user2)

			memcache.JSON.Set(gaeCtx, &memcache.Item{
				Key: user1.CacheID(),
				Object: appx.CachedEntity{
					Entity: user1,
					Key:    user1.Key(),
				},
			})

			memcache.JSON.Set(gaeCtx, &memcache.Item{
				Key: user2.CacheID(),
				Object: appx.CachedEntity{
					Entity: user2,
					Key:    user2.Key(),
				},
			})

			Convey("When I transform the incoming batch", func() {
				notCachedUser := NewUser(User{
					Name: "not cached",
					SSN:  "notcached",
				})

				userFromCache1 := NewUser(User{Name: user1.Name})
				userFromCache2 := NewUser(User{Name: user2.Name})

				batchItems := make(map[string]*appx.CachedEntity)
				batchItems[user1.CacheID()] = &appx.CachedEntity{
					Entity: userFromCache1,
				}
				batchItems[user2.CacheID()] = &appx.CachedEntity{
					Entity: userFromCache2,
				}
				batchItems[notCachedUser.CacheID()] = &appx.CachedEntity{
					Entity: notCachedUser,
				}

				in, out := rx.NewStream(1)
				out <- &appx.BatchCacheLoader{
					Keys:  []string{user1.CacheID(), user2.CacheID()},
					Items: batchItems,
				}

				close(out)

				stream := transformer.Transform(in)

				Convey("Then cache misses are sent downstream", func() {
					So(stream.Read(), ShouldResemble, []rx.T{notCachedUser})

					Convey("And entities are loaded from cache", func() {
						So(userFromCache1, ShouldResemble, user1)
						So(userFromCache2, ShouldResemble, user2)
					})
				})
			})
		})
	})
}
