# MongoDB store for [Session](https://github.com/go-session/session)

[![Build][Build-Status-Image]][Build-Status-Url] [![Codecov][codecov-image]][codecov-url] [![ReportCard][reportcard-image]][reportcard-url] [![GoDoc][godoc-image]][godoc-url] [![License][license-image]][license-url]

## Quick Start

### Download and install

```bash
go get -u -v github.com/go-session/mongo/v3
```

### Create file `server.go`

```go
package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/go-session/mongo/v3"
	"github.com/go-session/session/v3"
)

func main() {
	session.InitManager(
		session.SetStore(mongo.NewStore("mongodb://127.0.0.1:27017","test","session")),
	)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		store, err := session.Start(context.Background(), w, r)
		if err != nil {
			fmt.Fprint(w, err)
			return
		}

		store.Set("foo", "bar")
		err = store.Save()
		if err != nil {
			fmt.Fprint(w, err)
			return
		}

		http.Redirect(w, r, "/foo", 302)
	})

	http.HandleFunc("/foo", func(w http.ResponseWriter, r *http.Request) {
		store, err := session.Start(context.Background(), w, r)
		if err != nil {
			fmt.Fprint(w, err)
			return
		}

		foo, ok := store.Get("foo")
		if ok {
			fmt.Fprintf(w, "foo:%s", foo)
			return
		}
		fmt.Fprint(w, "does not exist")
	})

	http.ListenAndServe(":8080", nil)
}
```

### Build and run

```bash
$ go build server.go
$ ./server
```

### Open in your web browser

<http://localhost:8080>

    foo:bar

## MIT License

    Copyright (c) 2018 Lyric

[Build-Status-Url]: https://travis-ci.org/go-session/mongo
[Build-Status-Image]: https://travis-ci.org/go-session/mongo.svg?branch=master
[codecov-url]: https://codecov.io/gh/go-session/mongo
[codecov-image]: https://codecov.io/gh/go-session/mongo/branch/master/graph/badge.svg
[reportcard-url]: https://goreportcard.com/report/github.com/go-session/mongo
[reportcard-image]: https://goreportcard.com/badge/github.com/go-session/mongo
[godoc-url]: https://godoc.org/github.com/go-session/mongo
[godoc-image]: https://godoc.org/github.com/go-session/mongo?status.svg
[license-url]: http://opensource.org/licenses/MIT
[license-image]: https://img.shields.io/npm/l/express.svg
