package storage

import (
	"fmt"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		fmt.Printf(format, a...)
	}
	return
}

//implement reader for Project 1
type StandAloneReader struct {
	kvTxn 	*badger.Txn
	//raftTxn *badger.Txn
}

func NewStandAloneReader(kvTxn *badger.Txn) *StandAloneReader{
	return &StandAloneReader{
		kvTxn: kvTxn,
		//raftTxn: raftTxn,
	}
}

func (reader *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {

	value, err := engine_util.GetCFFromTxn(reader.kvTxn, cf, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		DPrintf("err = %v", err)
		return nil, err
	}
	return value, nil

}

func (reader *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	txn := reader.kvTxn
	return engine_util.NewCFIterator(cf, txn)
}

func (reader *StandAloneReader) Close() {
	reader.kvTxn.Discard()
	//reader.raftTxn.Discard()
}
