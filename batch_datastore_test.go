package appx_test

import (
	"github.com/drborges/appx"
	"github.com/drborges/rivers/stream"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
"github.com/drborges/rivers"
)

func TestBatchDatastore(t *testing.T) {
	Convey("Given I have an empty datastore batch of size 2", t, func() {
		batch := &appx.DatastoreBatch{Size: 2}
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
			in, out := stream.New(1)

			entity1 := NewUserWithFakeKey(User{Name: "entity1"})
			entity2 := NewUserWithFakeKey(User{Name: "entity2"})

			batch.Add(entity1)
			batch.Add(entity2)
			batch.Commit(stream.NewEmitter(rivers.NewContext(), out))
			close(out)

			Convey("Then a copy of the batch is sent to the output stream", func() {
				committedBatch := (<-in).(*appx.DatastoreBatch)
				So(committedBatch.Size, ShouldEqual, 2)
				So(committedBatch.Keys[0], ShouldEqual, entity1.Key())
				So(committedBatch.Keys[1], ShouldEqual, entity2.Key())
				So(committedBatch.Items[0], ShouldEqual, entity1)
				So(committedBatch.Items[1], ShouldEqual, entity2)

				Convey("And the batch is now empty", func() {
					So(batch.Empty(), ShouldBeTrue)
				})
			})
		})
	})
}
