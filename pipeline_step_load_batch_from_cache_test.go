package appx_test

import (
	"appengine/aetest"
	"appengine/memcache"
	"github.com/drborges/appx"
	"github.com/drborges/rivers"
	"github.com/drborges/rivers/stream"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestLoadBatchFromCache(t *testing.T) {
	gaeCtx, _ := aetest.NewContext(nil)
	defer gaeCtx.Close()

	Convey("Given I have a load batch from cache transformer", t, func() {
		riversCtx := rivers.NewContext()
		loadBatchProcessor := appx.NewStep(riversCtx).LoadBatchFromCache(gaeCtx)

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

				batch := &appx.MemcacheLoadBatch{
					Keys:  []string{user1.CacheID(), user2.CacheID()},
					Items: batchItems,
				}

				in, out := stream.New(1)
				loadBatchProcessor(batch, stream.NewEmitter(rivers.NewContext(), out))
				close(out)

				Convey("Then cache misses are sent downstream", func() {
					So(in.Read(), ShouldResemble, []stream.T{notCachedUser})

					Convey("And entities are loaded from cache", func() {
						So(userFromCache1, ShouldResemble, user1)
						So(userFromCache2, ShouldResemble, user2)
					})
				})
			})
		})
	})
}
