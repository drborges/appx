package appx

import (
	"github.com/drborges/riversv2/rx"
)

type transformer struct {
	context   rx.Context
	transform func(data rx.T) bool
}

func (transformer *transformer) Transform(in rx.InStream) rx.InStream {
	reader, writer := rx.NewStream(cap(in))

	go func() {
		defer transformer.context.Recover()
		defer close(writer)

		for {
			select {
			case <-transformer.context.Closed():
				return
			default:
				data, more := <-in
				if !more {
					return
				}

				// Send data downstream if there is still
				// some work to do with the data
				if transformer.transform(data) {
					writer <- data
				}
			}
		}
	}()

	return reader
}
