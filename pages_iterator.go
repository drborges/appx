package appx

import (
	"appengine"
	"appengine/datastore"
	"reflect"
)

type pageIterator struct {
	query      *datastore.Query
	context    appengine.Context
	nextCursor datastore.Cursor
	prevCursor datastore.Cursor
	started    bool
}

func NewPagesIterator(c appengine.Context, q *datastore.Query) Iterator {
	return &pageIterator{
		query:   q,
		context: c,
	}
}

// TODO refactor this mess :~
func (this *pageIterator) LoadNext(slice interface{}) error {
	sv := reflect.ValueOf(slice)
	if sv.Kind() != reflect.Ptr || sv.IsNil() || sv.Elem().Kind() != reflect.Slice {
		return ErrInvalidSliceType
	}
	sv = sv.Elem()

	elemType := sv.Type().Elem()
	if elemType.Kind() != reflect.Ptr || elemType.Elem().Kind() != reflect.Struct {
		return ErrInvalidEntityType
	}

	this.started = true
	iter := this.query.Run(this.context)
	for {
		dstValue := reflect.New(elemType.Elem())
		dst := dstValue.Interface()

		entity, ok := dst.(Entity)
		if !ok {
			return ErrInvalidEntityType
		}

		key, err := iter.Next(entity)
		if err == datastore.Done {
			cursor, cursorErr := iter.Cursor()
			if cursorErr != nil {
				return cursorErr
			}
			this.prevCursor = this.nextCursor
			this.nextCursor = cursor
			this.query = this.query.Start(this.nextCursor)
			if !this.HasNext() {
				this.prevCursor = datastore.Cursor{}
				this.nextCursor = datastore.Cursor{}
				return datastore.Done
			}
			break
		}

		if err != nil {
			return err
		}

		entity.SetKey(key)
		sv.Set(reflect.Append(sv, dstValue))
	}

	return nil
}

func (this *pageIterator) HasNext() bool {
	return !this.started || this.prevCursor.String() != this.nextCursor.String()
}

func (this *pageIterator) Cursor() string {
	return this.nextCursor.String()
}
