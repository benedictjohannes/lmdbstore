package lmdbstore

import (
	"errors"
	"fmt"
	"io/fs"
	"runtime"

	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/shamaton/msgpack/v2"
)

// LmdbEnvConfig is configuration for LmdbEnv
//
// There should be minimum 1 Databases []DbConfig entry
//
// Marshal and Unmarshal are optional
// and defaults to github.com/shamaton/msgpack
//
// All other fields should be set
type LmdbEnvConfig struct {
	OpenPath   string
	OpenFlag   uint
	OpenFSMode fs.FileMode
	MapSize    int64
	MaxReaders int
	// minimum 1 entry
	Databases []DbConfig
	// optional
	Marshal func(v interface{}) ([]byte, error)
	// optional
	Unmarshal func(data []byte, v interface{}) error
}

var DefaultLmdbConfig = LmdbEnvConfig{
	OpenPath:   ".",
	OpenFSMode: 0644,
	MapSize:    1 << 30,
	MaxReaders: 1,
	Databases:  []DbConfig{{DbName: "default"}},
	Marshal:    msgpack.MarshalAsArray,
	Unmarshal:  msgpack.UnmarshalAsArray,
}

// LmdbEnv wraps LmdbEnv with tested configurations
//
// LmdbEnv should always be created by calling NewLmdb(LmdbEnvConfig)
//
// Do not create the struct directly
//
type LmdbEnv struct {
	// Direct access to *lmdb.Env
	LmdbEnv          *lmdb.Env
	databases        map[string]*Db
	updateWorkerChan chan *dbOp
	quitChan         chan bool
	marshal          func(v interface{}) ([]byte, error)
	unmarshal        func(data []byte, v interface{}) error
}

// GetSingleDatabase returns a single database
//
// if Databases contains more than a single entry, GetSingleDatabase returns an error
//
func (l *LmdbEnv) GetSingleDatabase() (*Db, error) {
	if len(l.databases) != 1 {
		return nil, errors.New("number of databases configured LmdbEnv is not 1")
	}
	for _, v := range l.databases {
		return v, nil
	}
	return nil, errors.New("no database is configured in LmdbEnv")
}

// GetDatabase returns a database with name dbName
//
// Non-existing entries will return nil
//
func (l *LmdbEnv) GetDatabase(dbName string) *Db {
	return l.databases[dbName]
}

type dbOp struct {
	op  lmdb.TxnOp
	res chan<- error
}

// Db reperesents a single Database inside LmdbEnv
//
// Db should always be accessed through LmdbEnv.GetDatabase(dbName) or LmdbEnv.GetSingleDatabase()
//
// Do not create Db struct directly
//
type Db struct {
	dbi              lmdb.DBI
	lmdbEnv          *lmdb.Env
	updateWorkerChan chan *dbOp
	marshal          func(v interface{}) ([]byte, error)
	unmarshal        func(data []byte, v interface{}) error
}

// DbConfig is configuration that will be created as entries in LmdbEnv.Databases
//
// Marshal and Unmarshal are optional and defaults to the parent LmdbEnv's methods.
//
// Different Marshal and Unmarshal per database is possible,
// but should never change for the lifetime of the database.
//
type DbConfig struct {
	DbName    string
	Marshal   func(v interface{}) ([]byte, error)
	Unmarshal func(data []byte, v interface{}) error
}

// NewLmdb initialize a single LmdbEnv
//
// Initialized LmdbEnv would spawn a single "updater" goroutine for Update transactions
//
// The methods should be safe to use across multiple goroutines
//
func NewLmdb(config LmdbEnvConfig) (*LmdbEnv, error) {
	if len(config.Databases) < 1 {
		return nil, errors.New("no databases is setup")
	}
	lmdbEnv, err := lmdb.NewEnv()
	if err != nil {
		return nil, err
	}
	err = lmdbEnv.SetMapSize(config.MapSize)
	if err != nil {
		return nil, err
	}
	err = lmdbEnv.SetMaxDBs(len(config.Databases))
	if err != nil {
		return nil, err
	}
	err = lmdbEnv.SetMaxReaders(config.MaxReaders)
	if err != nil {
		return nil, err
	}
	err = lmdbEnv.Open(config.OpenPath, config.OpenFlag, config.OpenFSMode)
	if err != nil {
		return nil, err
	}
	lmdbHandler := LmdbEnv{
		LmdbEnv:          lmdbEnv,
		marshal:          config.Marshal,
		unmarshal:        config.Unmarshal,
		updateWorkerChan: make(chan *dbOp),
		databases:        make(map[string]*Db),
	}
	for _, dbConfig := range config.Databases {
		db := &Db{
			lmdbEnv:          lmdbEnv,
			updateWorkerChan: lmdbHandler.updateWorkerChan,
			marshal:          lmdbHandler.marshal,
			unmarshal:        lmdbHandler.unmarshal,
		}
		err = lmdbEnv.Update(func(txn *lmdb.Txn) error {
			db.dbi, err = txn.CreateDBI(dbConfig.DbName)
			return err
		})
		if err != nil {
			return nil, fmt.Errorf("error creating database %s", dbConfig.DbName)
		}
		lmdbHandler.databases[dbConfig.DbName] = db
	}
	go func() {
		runtime.LockOSThread()
		defer runtime.LockOSThread()
		for {
			select {
			case op := <-lmdbHandler.updateWorkerChan:
				{
					op.res <- lmdbEnv.UpdateLocked(op.op)
				}
			case <-lmdbHandler.quitChan:
				{
					lmdbEnv.Sync(true)
					lmdbEnv.Close()
					runtime.UnlockOSThread()
				}
			}
		}
	}()
	return &lmdbHandler, nil
}

// Close flushes the Lmdb databases to disk and stop the updater goroutine
//
// Note that closed LmdbEnv should not be used for any transactions
//
func (e *LmdbEnv) Close() {
	e.quitChan <- false
}

// UpdateTxn runs a lmdb.TxnOp inside the updater goroutine
//
// The call will block until the transaction is finished
//
func (s *Db) UpdateTxn(op lmdb.TxnOp) error {
	res := make(chan error)
	s.updateWorkerChan <- &dbOp{op, res}
	err := <-res
	return err
}

// Put a value with key inside the database
//
// The call will block until the transaction is finished
//
func (s *Db) Put(key []byte, value interface{}) error {
	return s.UpdateTxn(func(txn *lmdb.Txn) error {
		switch v := value.(type) {
		case []byte:
			return txn.Put(s.dbi, key, v, 0)
		default:
			{
				b, err := s.marshal(v)
				if err != nil {
					return err
				}
				return txn.Put(s.dbi, key, b, 0)
			}
		}
	})
}

var zeroLengthBytes = make([]byte, 0)

// Del a value with key inside the database
//
// The call will block until the transaction is finished
//
func (s *Db) Del(key []byte) error {
	return s.UpdateTxn(func(txn *lmdb.Txn) error {
		return txn.Del(s.dbi, key, zeroLengthBytes)
	})
}

// Drop empties a database
//
// The call will block until the transaction is finished
//
func (s *Db) Drop() error {
	return s.UpdateTxn(func(txn *lmdb.Txn) error {
		return txn.Drop(s.dbi, false)
	})
}

// Get returns the binary value at key inside the database
//
// If the key does not exist, an error is returned
//
// The returned value is copied for safe use outside the lmdb.TxnOp
//
func (s *Db) Get(key []byte) (b []byte, err error) {
	err = s.lmdbEnv.View(func(txn *lmdb.Txn) (err error) {
		bOri, err := txn.Get(s.dbi, key)
		if err != nil {
			return err
		}
		copy(b, bOri)
		return nil
	})
	return b, err
}

// GetAndMarshal marshals value at key into &dest
//
// If the key does not exist, an error is returned
//
// The value is first copied for safe use outside the lmdb.TxnOp
//
// Returned value is safe to use across goroutines
//
func (s *Db) GetAndMarshal(key []byte, dest interface{}) (err error) {
	return s.lmdbEnv.View(func(txn *lmdb.Txn) error {
		bOri, err := txn.Get(s.dbi, key)
		if err != nil {
			return err
		}
		if len(bOri) == 0 {
			return errors.New("zero length bytes from database")
		}
		b := make([]byte, len(bOri))
		copy(b, bOri)
		err = s.unmarshal(b, &dest)
		return err
	})
}
