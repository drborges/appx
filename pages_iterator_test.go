package appx_test

import (
	"appengine/aetest"
	"appengine/datastore"
	"github.com/drborges/appxv2"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestPagesIterator(t *testing.T) {
	c, _ := aetest.NewContext(nil)
	defer c.Close()

	tags := []*Tag{
		&Tag{Name: "golang", Owner: "Borges"},
		&Tag{Name: "ruby", Owner: "Borges"},
		&Tag{Name: "scala", Owner: "Borges"},
		&Tag{Name: "swift", Owner: "Diego"},
	}

	createAll(c, tags...)

	Convey("PagesIterator", t, func() {
		Convey("Given I have a pages iterator with 2 pages each with 2 items", func() {
			q := datastore.NewQuery(new(Tag).KeySpec().Kind).Limit(2)
			iter := appx.NewDatastore(c).Query(q).PagesIterator()

			Convey("Then I can load the first page", func() {
				firstPage := []*Tag{}
				So(iter.Cursor(), ShouldBeEmpty)
				So(iter.LoadNext(&firstPage), ShouldBeNil)
				So(iter.HasNext(), ShouldBeTrue)
				So(firstPage, ShouldResemble, tags[0:2])

				Convey("Then I can load the second page", func() {
					secondPage := []*Tag{}
					So(iter.Cursor(), ShouldNotBeEmpty)
					So(iter.LoadNext(&secondPage), ShouldBeNil)
					So(iter.HasNext(), ShouldBeTrue)
					So(secondPage, ShouldResemble, tags[2:])

					Convey("Then I cannot load more pages", func() {
						page := []*Tag{}
						So(iter.LoadNext(&page), ShouldEqual, datastore.Done)
						So(iter.HasNext(), ShouldBeFalse)
						So(iter.Cursor(), ShouldBeEmpty)
						So(page, ShouldBeEmpty)
					})
				})
			})
		})

		Convey("Given I have a pages iterator with 4 pages each with 1 item", func() {
			q := datastore.NewQuery(new(Tag).KeySpec().Kind).Limit(1)
			iter := appx.NewDatastore(c).Query(q).PagesIterator()

			Convey("Then I can load pages until iterator has no more pages", func() {
				pages := [][]*Tag{}
				for iter.HasNext() {
					page := []*Tag{}
					if err := iter.LoadNext(&page); err == nil {
						pages = append(pages, page)
					}
				}

				So(len(pages), ShouldEqual, 4)
				So(pages[0], ShouldResemble, []*Tag{tags[0]})
				So(pages[1], ShouldResemble, []*Tag{tags[1]})
				So(pages[2], ShouldResemble, []*Tag{tags[2]})
				So(pages[3], ShouldResemble, []*Tag{tags[3]})
			})
		})

		Convey("Given I have a pages iterator with cursor starting from the second and last page", func() {
			q := datastore.NewQuery(new(Tag).KeySpec().Kind).Limit(2)
			firstPage := &[]*Tag{}
			prevIter := appx.NewDatastore(c).Query(q).PagesIterator()
			prevIter.LoadNext(firstPage)

			iterStartingFromSecondPage := appx.NewDatastore(c).Query(q).StartFrom(prevIter.Cursor()).PagesIterator()

			Convey("Then I can load the page", func() {
				secondPage := []*Tag{}
				So(iterStartingFromSecondPage.LoadNext(&secondPage), ShouldBeNil)
				So(iterStartingFromSecondPage.HasNext(), ShouldBeTrue)
				So(iterStartingFromSecondPage.Cursor(), ShouldNotBeEmpty)
				So(secondPage, ShouldResemble, tags[2:])

				Convey("Then I cannot load more pages", func() {
					page := []*Tag{}
					So(iterStartingFromSecondPage.LoadNext(&page), ShouldEqual, datastore.Done)
					So(iterStartingFromSecondPage.HasNext(), ShouldBeFalse)
					So(iterStartingFromSecondPage.Cursor(), ShouldBeEmpty)
					So(page, ShouldBeEmpty)
				})
			})
		})

		Convey("Given I have a pages iterator with zero items", func() {
			q := datastore.NewQuery(new(Tag).KeySpec().Kind).Filter("Owner=", "non existent").Limit(1)
			iter := appx.NewDatastore(c).Query(q).PagesIterator()

			Convey("When I load the next page", func() {
				firstPage := []*Tag{}
				So(iter.LoadNext(&firstPage), ShouldEqual, datastore.Done)

				Convey("Then the page is empty", func() {
					So(firstPage, ShouldBeEmpty)

					Convey("Then the cursor is empty", func() {
						So(iter.Cursor(), ShouldBeEmpty)

						Convey("Then it has no more results", func() {
							So(iter.HasNext(), ShouldBeFalse)
						})
					})
				})
			})
		})
	})
}
