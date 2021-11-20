package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engines *engine_util.Engines
	config  *config.Config
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (standAloneStorageReader StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(standAloneStorageReader.txn, cf, key)
	if err != nil && err != badger.ErrKeyNotFound {
		return nil, err
	}
	return val, nil
}

func (standAloneStorageReader StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, standAloneStorageReader.txn)
}

func (standAloneStorageReader StandAloneStorageReader) Close() {
	standAloneStorageReader.txn.Discard()
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{config: conf}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	path := s.config.DBPath
	kvPath := filepath.Join(path, "kv")
	raftPath := filepath.Join(path, "raft")
	kvDb := engine_util.CreateDB(kvPath, false)
	raftDb := engine_util.CreateDB(raftPath, true)
	s.engines = engine_util.NewEngines(kvDb, raftDb, kvPath, raftPath)
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.engines.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StandAloneStorageReader{txn: s.engines.Kv.NewTransaction(false)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	writeBatchs := engine_util.WriteBatch{}
	for _, v := range batch {
		if v.Value() != nil {
			//set
			writeBatchs.SetCF(v.Cf(), v.Key(), v.Value())
		} else {
			//delete
			writeBatchs.DeleteCF(v.Cf(), v.Key())
		}
	}
	return writeBatchs.WriteToDB(s.engines.Kv)
}
