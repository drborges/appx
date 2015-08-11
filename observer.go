package appx

import (
	"github.com/drborges/riversv2/rx"
)

type observer struct {
	context    rx.Context
	onData     func(data rx.T, out rx.OutStream)
	onComplete func(out rx.OutStream)
}

func (observer *observer) Transform(in rx.InStream) rx.InStream {
	reader, writer := rx.NewStream(cap(in))

	go func() {
		defer observer.context.Recover()
		defer close(writer)

		for {
			select {
			case <-observer.context.Closed():
				return
			default:
				data, more := <-in
				if !more {
					observer.onComplete(writer)
					return
				}

				observer.onData(data, writer)
			}
		}
	}()

	return reader
}
