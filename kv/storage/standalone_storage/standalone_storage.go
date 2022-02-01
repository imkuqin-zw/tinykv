package standalone_storage

import (
	"errors"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	DBPath string
	Raft   bool
	db     *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	s := &StandAloneStorage{
		DBPath: conf.DBPath,
		Raft:   conf.Raft,
	}
	return s
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	s.db = engine_util.CreateDB(s.DBPath, s.Raft)
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return s, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wb := new(engine_util.WriteBatch)
	for _, item := range batch {
		switch item.Data.(type) {
		case storage.Put:
			wb.SetCF(item.Cf(), item.Key(), item.Value())
		case storage.Delete:
			wb.DeleteCF(item.Cf(), item.Key())
		}
	}
	return wb.WriteToDB(s.db)
}

func (s *StandAloneStorage) GetCF(cf string, key []byte) ([]byte, error) {
	data, err := engine_util.GetCF(s.db, cf, key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	return data, err
}

func (s *StandAloneStorage) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.db.NewTransaction(true))
}

func (s *StandAloneStorage) Close() {
	return
}
