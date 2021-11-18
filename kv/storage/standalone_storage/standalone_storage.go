package standalone_storage

import (
	"github.com/Connor1996/badger"
	"path/filepath"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines
	config *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dbPath := conf.DBPath
	kvPath := filepath.Join(dbPath, "kv")
	raftPath := filepath.Join(dbPath, "raft")

	//os.MkdirAll(kvPath, os.ModePerm)
	//os.MkdirAll(raftPath, os.ModePerm)

	kvDB := engine_util.CreateDB("kv", false)
	raftDB := engine_util.CreateDB("raft", true)
	return &StandAloneStorage{
		engine: engine_util.NewEngines(kvDB, raftDB, kvPath, raftPath),
		config: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	// nothing to start
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engine.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	// After implementing the standalone reader, we should start a new transaction in order to create the reader
	// for read operation, no need for updating, set the "update" false
	kvTxn := s.engine.Kv.NewTransaction(false)
	reader := storage.NewStandAloneReader(kvTxn)
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	// assert the type of batch, then do the relevant work: put and delete
	// work can be implemented by the functions in util/engine_util/util, in this case, use PutCF and DeleteCF
	/*for _, batch := range batch {
		switch batch.Data.(type) {
		case storage.Put:
			put := batch.Data.(storage.Put)
			engine_util.PutCF(s.engine.Kv, put.Cf, put.Key, put.Value)
		case storage.Delete:
			delete := batch.Data.(storage.Delete)
			engine_util.DeleteCF(s.engine.Kv, delete.Cf, delete.Key)
		}
	}
	return nil*/
	wb := new(engine_util.WriteBatch)
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			put := m.Data.(storage.Put)
			//log.Printf("write put: %v, detail: cf=%v, key=%v, val=%v", put, put.Cf, put.Key, put.Value)
			wb.SetCF(put.Cf, put.Key, put.Value)
		case storage.Delete:
			delete := m.Data.(storage.Delete)
			wb.DeleteCF(delete.Cf, delete.Key)
		}
	}
	err := wb.WriteToDB(s.engine.Kv)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	}
	return nil
}
