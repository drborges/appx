package appx_test

import (
	"appengine/aetest"
	"appengine/datastore"
	"github.com/drborges/appx"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestItemsIterator(t *testing.T) {
	c, _ := aetest.NewContext(nil)
	defer c.Close()

	tags := []*Tag{
		&Tag{Name: "golang", Owner: "Borges"},
		&Tag{Name: "ruby", Owner: "Borges"},
		&Tag{Name: "scala", Owner: "Borges"},
		&Tag{Name: "swift", Owner: "Diego"},
	}

	createAll(c, tags...)

	// TODO fix non deterministic issue
	Convey("ItemsIterator", t, func() {
		Convey("Given I have an items iterator with 3 pages each with 1 item", func() {
			q := datastore.NewQuery(new(Tag).KeySpec().Kind).Filter("Owner=", "Borges").Limit(1)
			iter := appx.NewDatastore(c).Query(q).ItemsIterator()
			tagsFromIterator := []*Tag{&Tag{}, &Tag{}, &Tag{}}

			Convey("Then I can load the first item", func() {
				So(iter.HasNext(), ShouldBeTrue)
				So(iter.Cursor(), ShouldBeEmpty)
				So(iter.LoadNext(tagsFromIterator[0]), ShouldBeNil)
				So(iter.HasNext(), ShouldBeTrue)
				So(iter.Cursor(), ShouldNotBeEmpty)
				So(tagsFromIterator[0], ShouldResemble, tags[0])

				Convey("Then I can load the second item", func() {
					So(iter.LoadNext(tagsFromIterator[1]), ShouldBeNil)
					So(iter.HasNext(), ShouldBeTrue)
					So(iter.Cursor(), ShouldNotBeEmpty)
					So(tagsFromIterator[1], ShouldResemble, tags[2])

					Convey("Then I can load the third item", func() {
						So(iter.LoadNext(tagsFromIterator[2]), ShouldBeNil)
						So(iter.HasNext(), ShouldBeTrue)
						So(iter.Cursor(), ShouldNotBeEmpty)
						So(tagsFromIterator[2], ShouldResemble, tags[1])

						Convey("Then I cannot load more items", func() {
							So(iter.LoadNext(&Tag{}), ShouldEqual, datastore.Done)
							So(iter.HasNext(), ShouldBeFalse)
							So(iter.Cursor(), ShouldBeEmpty)
						})
					})
				})

				Convey("I can create a new iterator using the cursor from the previous one", func() {
					iterWithCursor := appx.NewDatastore(c).Query(q).StartFrom(iter.Cursor()).ItemsIterator()

					Convey("I can load the second item", func() {
						So(iterWithCursor.LoadNext(tagsFromIterator[1]), ShouldBeNil)
						So(iterWithCursor.HasNext(), ShouldBeTrue)
						So(iter.Cursor(), ShouldNotBeEmpty)
						So(tagsFromIterator[1], ShouldResemble, tags[1])
					})
				})
			})

			Convey("Then I can load items until iterator has no more items", func() {
				items := []*Tag{}
				for iter.HasNext() {
					item := &Tag{}
					if err := iter.LoadNext(item); err == nil {
						items = append(items, item)
					}
				}

				So(len(items), ShouldEqual, 3)
				So(items[0], ShouldResemble, tags[0])
				So(items[1], ShouldResemble, tags[1])
				So(items[2], ShouldResemble, tags[2])
			})
		})

		Convey("Given I have an items iterator with zero items", func() {
			q := datastore.NewQuery(new(Tag).KeySpec().Kind).Filter("Owner=", "non existent").Limit(1)
			iter := appx.NewDatastore(c).Query(q).ItemsIterator()

			Convey("When I load the next item", func() {
				firstItem := Tag{}
				So(iter.Cursor(), ShouldBeEmpty)
				So(iter.LoadNext(&firstItem), ShouldEqual, datastore.Done)
				So(iter.Cursor(), ShouldBeEmpty)

				Convey("Then the item is not populated", func() {
					So(firstItem, ShouldResemble, Tag{})

					Convey("Then it has no more results", func() {
						So(iter.HasNext(), ShouldBeFalse)
					})
				})
			})
		})
	})
}
