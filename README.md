# Introduction

The package `lmdbstore` wraps the [github.com/bmatsuo/lmdb-go/lmdb](github.com/bmatsuo/lmdb-go/lmdb) with convenient methods and defaults:

-   Multiple databases within one lmdb.Env
-   One goroutine that handles updates
-   Convenient per database Get, Put, Del, and Drop methods
-   Customizable (per `lmdb.Env` or database) `Marshal` and `Unmarshal` methods
-   Defaults to the performant [github.com/shamaton/msgpack/v2](github.com/shamaton/msgpack/v2) for `Marshal` and `Unmarshal`

# Usage

For a single database use case on the directory where the go program is in,

```go
package main

import (
    "gitlab.com/benedictjohannes/lmbdbstore"
)
var db *lmdbstore.Db

func initLmdbStore() error {
    // initialize LmdbEnv
	lmdbEnv, err := lmdbstore.NewLmdb(lmdbstore.DefaultLmdbConfig)
	if err != nil {
        return err
	}
	// get the default database
	db, err = lmdbEnv.GetSingleDatabase()
	return err
}
type example struct {
    Id          int
    Description string
}
func main() {
	err := initLmdbStore()
	if err != nil {
		fmt.Println("Error initiating LMDB database: ", err)
		return
	}
	err = db.Put([]byte("your key"), example{888, "Fortune Cookies"})
	if err != nil {
		fmt.Println("Failed to save into database: ", err)
		return
	}
	var exampleVar example
	err = db.GetAndMarshal([]byte("your key"), &exampleVar)
	if err != nil {
		fmt.Println("Failed to get saved avlue from database: ", err)
		return
	}
	fmt.Println(exampleVar)
}
```

# Another wrapper?

Using `lmdb` directly almost requires either writing a lot of wrapper methods or duplicating code across users of the database. The defaults this package initializes for its callers should be adequate for most use cases, while the convenience methods can make interacting with `lmdb` easier in Go programs.