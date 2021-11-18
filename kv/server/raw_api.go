package server

import (
	"context"
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		fmt.Printf(format, a...)
	}
	return
}

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}else {
		defer reader.Close()
		value, err := reader.GetCF(req.GetCf(), req.GetKey())
		if err != nil {
			DPrintf("test comes 1, and err = %v", err)
			return nil, err
		}
		DPrintf("test comes 2, and value = %v", value)
		return &kvrpcpb.RawGetResponse{RegionError: nil, Value: value, NotFound: value == nil}, nil
	}
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	resp := &kvrpcpb.RawPutResponse{}
	/*put := storage.Modify{Data: storage.Put{Cf: req.Cf, Key: req.Key, Value: req.Value}}
	batch := make([]storage.Modify, 0)
	batch = append(batch, put)*/
	batch := []storage.Modify{storage.Modify{
		Data: storage.Put{Cf: req.Cf, Key: req.Key, Value: req.Value},
	}}

	err := server.storage.Write(req.Context, batch)
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	// Similar to RawPut , just change the type of Modify
	/*resp := &kvrpcpb.RawDeleteResponse{}
	delete := storage.Modify{Data: storage.Delete{Cf: req.Cf, Key: req.Key}}
	batch := make([]storage.Modify, 0)
	batch = append(batch, delete)*/
	resp := &kvrpcpb.RawDeleteResponse{}
	batch := []storage.Modify{storage.Modify{
		Data: storage.Delete{Cf: req.Cf, Key: req.Key},
	}}
	err := server.storage.Write(req.Context, batch)
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	iter := reader.IterCF(req.Cf)
	iter.Seek(req.StartKey)
	limit := req.Limit
	var kvPairs []*kvrpcpb.KvPair
	for i:=0; iter.Valid() && i < int(limit); i++ {
		item := iter.Item()
		pair := new(kvrpcpb.KvPair)
		pair.Key = item.Key()
		pair.Value, err = item.Value()
		/*if pair.Value == nil {
			iter.Next()
			continue
		}*/
		if err != nil {
			 return nil, err
		}
		DPrintf("kvPairs: key=[%v], val=[%v] \n", pair.Key, pair.Value)
		kvPairs = append(kvPairs, pair)
		iter.Next()
	}
	iter.Close()

	return &kvrpcpb.RawScanResponse{RegionError: nil, Kvs: kvPairs}, nil
}
