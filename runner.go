package appx

import (
	"appengine"
	"appengine/datastore"
	"github.com/drborges/rivers"
	"github.com/drborges/rivers/producers"
	"github.com/drborges/rivers/stream"
	"reflect"
)

type runner struct {
	context appengine.Context
	query   *datastore.Query
}

func (runner *runner) Count() (int, error) {
	return runner.query.Count(runner.context)
}

func (runner *runner) Results(slice interface{}) error {
	return runner.PagesIterator().LoadNext(slice)
}

func (runner *runner) Result(e Entity) error {
	return runner.ItemsIterator().LoadNext(e)
}

func (runner *runner) StartFrom(cursor string) *runner {
	c, _ := datastore.DecodeCursor(cursor)
	runner.query = runner.query.Start(c)
	return runner
}

func (runner *runner) ItemsIterator() Iterator {
	return NewItemsIterator(runner.context, runner.query)
}

func (runner *runner) PagesIterator() Iterator {
	return NewPagesIterator(runner.context, runner.query)
}

func (runner *runner) StreamOf(dst interface{}) *rivers.Stream {
	return rivers.From(&producers.Observable{
		Capacity: 2000,
		Emit: func(emitter stream.Emitter) {
			iter := runner.ItemsIterator()
			for iter.HasNext() {
				data := reflect.New(reflect.TypeOf(dst)).Interface()
				if err := iter.LoadNext(data); err != nil {
					return
				}
				emitter.Emit(data)
			}
		},
	})
}
