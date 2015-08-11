package appx_test

import (
	"appengine/aetest"
	"appengine/datastore"
	"github.com/drborges/appxv2"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestQuery(t *testing.T) {
	c, _ := aetest.NewContext(nil)
	defer c.Close()

	golang := &Tag{Name: "golang", Owner: "Borges"}
	swift := &Tag{Name: "swift", Owner: "Borges"}
	ruby := &Tag{Name: "ruby", Owner: "Diego"}

	createAll(c, golang, swift, ruby)

	Convey("Given I have a QueryRunner", t, func() {
		q := datastore.NewQuery(new(Tag).KeySpec().Kind).Filter("Owner=", "Borges")
		runner := appx.NewDatastore(c).Query(q)

		Convey("When I run Results", func() {
			result := []*Tag{}
			err := runner.Results(&result)

			Convey("Then it succeeds", func() {
				So(err, ShouldBeNil)

				Convey("Then it loads the matched entities into the given slice", func() {
					So(result, ShouldResemble, []*Tag{golang, swift})

					Convey("Then it sets keys back to the entities", func() {
						So(result[0].Key(), ShouldNotBeNil)
						So(result[1].Key(), ShouldNotBeNil)
					})
				})
			})
		})

		Convey("When I run Result", func() {
			tag := &Tag{}
			err := runner.Result(tag)

			Convey("Then it succeeds", func() {
				So(err, ShouldBeNil)

				Convey("Then it loads data into the given entity", func() {
					So(tag, ShouldResemble, golang)

					Convey("Then it sets the key back to the entity", func() {
						So(tag.Key(), ShouldNotBeNil)
					})
				})
			})
		})

		Convey("When I run Count", func() {
			count, err := runner.Count()

			Convey("Then it succeeds", func() {
				So(err, ShouldBeNil)

				Convey("Then count is 2", func() {
					So(count, ShouldEqual, 2)
				})
			})
		})
	})
}