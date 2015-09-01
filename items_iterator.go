package appx

import (
	"appengine"
	"appengine/datastore"
)

type itemsIterator struct {
	query              *datastore.Query
	context            appengine.Context
	nextCursor         datastore.Cursor
	prevCursor         datastore.Cursor
	doneProcessingPage bool
}

func NewItemsIterator(c appengine.Context, q *datastore.Query) Iterator {
	return &itemsIterator{
		query:              q,
		context:            c,
		nextCursor:         datastore.Cursor{},
		prevCursor:         datastore.Cursor{},
		doneProcessingPage: false,
	}
}

func (this *itemsIterator) LoadNext(dst interface{}) error {
	e, ok := dst.(Entity)
	if !ok {
		return ErrInvalidEntityType
	}

	iter := this.query.Run(this.context)
	key, err := iter.Next(e)

	if _, missingField := err.(*datastore.ErrFieldMismatch); !missingField && err != nil && err != datastore.Done {
		return err
	}

	this.doneProcessingPage = err != nil && err == datastore.Done

	if !this.HasNext() {
		this.prevCursor = datastore.Cursor{}
		this.nextCursor = datastore.Cursor{}
		return datastore.Done
	}

	cursor, err := iter.Cursor()
	if err != nil {
		return err
	}
	this.prevCursor = this.nextCursor
	this.nextCursor = cursor
	this.query = this.query.Start(cursor)

	if this.doneProcessingPage {
		return this.LoadNext(e)
	}

	e.SetKey(key)
	return nil
}

func (this *itemsIterator) HasNext() bool {
	return !this.doneProcessingPage || this.prevCursor != this.nextCursor
}

func (this *itemsIterator) Cursor() string {
	return this.nextCursor.String()
}
