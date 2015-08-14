package appx_test

import (
	"encoding/json"
	"github.com/drborges/appx"
	"github.com/drborges/riversv2/rx"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func toJSON(e appx.Entity) []byte {
	cachedEntity := &appx.CachedEntity{
		Entity: e,
		Key:    e.Key(),
	}
	json, _ := json.Marshal(cachedEntity)
	return json
}

func TestBatchCacheSetter(t *testing.T) {
	Convey("Given I have an empty batch of size 2", t, func() {
		batch := &appx.BatchCacheSetter{Size: 2}
		So(batch.Empty(), ShouldBeTrue)
		So(batch.Full(), ShouldBeFalse)

		Convey("When I add an entity to the batch", func() {
			batch.Add(NewUserWithFakeKey(User{Name: "borges"}))

			Convey("Then the batch is no longer empty", func() {
				So(batch.Empty(), ShouldBeFalse)

				Convey("And it is not yet full", func() {
					So(batch.Full(), ShouldBeFalse)
				})
			})
		})

		Convey("When I add enough entities", func() {
			batch.Add(NewUserWithFakeKey(User{Name: "borges"}))
			batch.Add(NewUserWithFakeKey(User{Name: "diego"}))

			Convey("Then the batch is full", func() {
				So(batch.Full(), ShouldBeTrue)
			})
		})

		Convey("When I commit the batch", func() {
			in, out := rx.NewStream(1)

			entity1 := NewUserWithFakeKey(User{
				Name: "entity1",
				SSN:  "123123",
			})

			entity2 := NewUserWithFakeKey(User{
				Name: "entity2",
				SSN:  "321321",
			})

			batch.Add(entity1)
			batch.Add(entity2)
			batch.Commit(out)
			close(out)

			Convey("Then a copy of the batch is sent to the output stream", func() {
				committedBatch := (<-in).(*appx.BatchCacheSetter)
				So(committedBatch.Size, ShouldEqual, 2)
				So(committedBatch.Items[0].Key, ShouldEqual, entity1.CacheID())
				So(committedBatch.Items[1].Key, ShouldEqual, entity2.CacheID())
				So(committedBatch.Items[0].Value, ShouldResemble, toJSON(entity1))
				So(committedBatch.Items[1].Value, ShouldResemble, toJSON(entity2))

				Convey("And the batch is now empty", func() {
					So(batch.Empty(), ShouldBeTrue)
				})
			})
		})
	})
}

func TestBatchCacheLoader(t *testing.T) {
	Convey("Given I have an empty batch of size 2", t, func() {
		batch := &appx.BatchCacheLoader{Size: 2}
		So(batch.Empty(), ShouldBeTrue)
		So(batch.Full(), ShouldBeFalse)

		Convey("When I add an entity to the batch", func() {
			batch.Add(NewUserWithFakeKey(User{Name: "borges"}))

			Convey("Then the batch is no longer empty", func() {
				So(batch.Empty(), ShouldBeFalse)

				Convey("And it is not yet full", func() {
					So(batch.Full(), ShouldBeFalse)
				})
			})
		})

		Convey("When I add enough entities", func() {
			batch.Add(NewUserWithFakeKey(User{Name: "borges"}))
			batch.Add(NewUserWithFakeKey(User{Name: "diego"}))

			Convey("Then the batch is full", func() {
				So(batch.Full(), ShouldBeTrue)
			})
		})

		Convey("When I commit the batch", func() {
			in, out := rx.NewStream(1)

			entity1 := NewUserWithFakeKey(User{
				Name: "entity1",
				SSN:  "123123",
			})

			entity2 := NewUserWithFakeKey(User{
				Name: "entity2",
				SSN:  "321321",
			})

			batch.Add(entity1)
			batch.Add(entity2)
			batch.Commit(out)
			close(out)

			Convey("Then a copy of the batch is sent to the output stream", func() {
				committedBatch := (<-in).(*appx.BatchCacheLoader)
				So(committedBatch.Size, ShouldEqual, 2)
				So(committedBatch.Keys[0], ShouldEqual, entity1.CacheID())
				So(committedBatch.Keys[1], ShouldEqual, entity2.CacheID())
				So(committedBatch.Items[entity1.CacheID()], ShouldResemble, &appx.CachedEntity{Entity: entity1})
				So(committedBatch.Items[entity2.CacheID()], ShouldResemble, &appx.CachedEntity{Entity: entity2})

				Convey("And the batch is now empty", func() {
					So(batch.Empty(), ShouldBeTrue)
				})
			})
		})
	})
}

func TestBatchCacheDeleter(t *testing.T) {
	Convey("Given I have an empty batch of size 2", t, func() {
		batch := &appx.BatchCacheDeleter{Size: 2}
		So(batch.Empty(), ShouldBeTrue)
		So(batch.Full(), ShouldBeFalse)

		Convey("When I add an entity to the batch", func() {
			batch.Add(NewUserWithFakeKey(User{Name: "borges"}))

			Convey("Then the batch is no longer empty", func() {
				So(batch.Empty(), ShouldBeFalse)

				Convey("And it is not yet full", func() {
					So(batch.Full(), ShouldBeFalse)
				})
			})
		})

		Convey("When I add enough entities", func() {
			batch.Add(NewUserWithFakeKey(User{Name: "borges"}))
			batch.Add(NewUserWithFakeKey(User{Name: "diego"}))

			Convey("Then the batch is full", func() {
				So(batch.Full(), ShouldBeTrue)
			})
		})

		Convey("When I commit the batch", func() {
			in, out := rx.NewStream(1)

			entity1 := NewUserWithFakeKey(User{
				Name: "entity1",
				SSN:  "123123",
			})

			entity2 := NewUserWithFakeKey(User{
				Name: "entity2",
				SSN:  "321321",
			})

			batch.Add(entity1)
			batch.Add(entity2)
			batch.Commit(out)
			close(out)

			Convey("Then a copy of the batch is sent to the output stream", func() {
				committedBatch := (<-in).(*appx.BatchCacheDeleter)
				So(committedBatch.Size, ShouldEqual, 2)
				So(committedBatch.Keys[0], ShouldEqual, entity1.CacheID())
				So(committedBatch.Keys[1], ShouldEqual, entity2.CacheID())

				Convey("And the batch is now empty", func() {
					So(batch.Empty(), ShouldBeTrue)
				})
			})
		})
	})
}
