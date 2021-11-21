package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
// KV Get will get the latest version of the value and if the value is locked it will return false
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	response := &kvrpcpb.GetResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			response.RegionError = regionErr.RequestErr
		}
		return response, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(req.Key) //lock is not null if the key is locked
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			response.RegionError = regionErr.RequestErr
		}
		return response, err
	}
	// if the key is locked return error
	if lock != nil && req.Version >= lock.Ts {
		response.Error = &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         req.Key,
				LockTtl:     lock.Ttl,
			},
		}
		return response, nil
	}
	value, err := txn.GetValue(req.Key)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			response.RegionError = regionErr.RequestErr
		}
		return response, err
	}
	if value == nil {
		response.NotFound = true
		return response, nil
	}
	response.Value = value
	return response, nil
}

// if none of the keys are locked ,it will lock all the keys and write the deafult cf
func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	response := &kvrpcpb.PrewriteResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			response.RegionError = regionErr.RequestErr
			return response, nil
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	var keyErrors []*kvrpcpb.KeyError
	for _, mutation := range req.Mutations {
		writeOject, timeStamped, err := txn.MostRecentWrite(mutation.Key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				response.RegionError = regionErr.RequestErr
				return response, nil
			}
			return response, nil
		}
		//check if there is any write conflict
		if writeOject != nil && timeStamped >= req.StartVersion {
			keyErrors = append(keyErrors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: timeStamped,
					Key:        mutation.Key,
					Primary:    req.PrimaryLock,
				}})
			continue
		}

		lock, err := txn.GetLock(mutation.Key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				response.RegionError = regionErr.RequestErr
				return response, nil
			}
			return nil, err
		}
		if lock != nil && lock.Ts != req.StartVersion {
			keyErrors = append(keyErrors, &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: req.PrimaryLock,
					LockVersion: lock.Ts,
					LockTtl:     lock.Ttl,
					Key:         mutation.Key,
				}})
			continue
		}
		//ready to write
		if mutation.Op == kvrpcpb.Op_Put {
			txn.PutValue(mutation.Key, mutation.Value)
			txn.PutLock(mutation.Key, &mvcc.Lock{
				Primary: req.PrimaryLock,
				Ts:      req.StartVersion,
				Ttl:     req.LockTtl,
				Kind:    mvcc.WriteKindPut,
			})
		} else if mutation.Op == kvrpcpb.Op_Del {
			txn.DeleteValue(mutation.Key)
			txn.PutLock(mutation.Key, &mvcc.Lock{
				Primary: req.PrimaryLock,
				Ts:      req.StartVersion,
				Ttl:     req.LockTtl,
				Kind:    mvcc.WriteKindDelete,
			})
		}
	}

	if len(keyErrors) > 0 {
		response.Errors = keyErrors
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			response.RegionError = regionErr.RequestErr
			return response, nil
		}
		return response, err
	}
	return response, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	response := &kvrpcpb.CommitResponse{}
	if len(req.Keys) == 0 {
		return response, nil
	}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			response.RegionError = regionErr.RequestErr
			return response, err
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	for _, key := range req.Keys {
		//get the lock of the transaction
		lock, err := txn.GetLock(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				response.RegionError = regionErr.RequestErr
				return response, nil
			}
			return nil, err
		}
		if lock == nil {
			continue
		}
		// the key is locked by different transaction
		if req.StartVersion != lock.Ts {
			response.Error = &kvrpcpb.KeyError{Retryable: "true"}
			write, _, err := txn.CurrentWrite(key)
			if err != nil {
				if regionErr, ok := err.(*raft_storage.RegionError); ok {
					response.RegionError = regionErr.RequestErr
					return response, nil
				}
				return nil, err
			}
			if write != nil {
				if write.Kind == mvcc.WriteKindRollback {
					response.Error = &kvrpcpb.KeyError{Retryable: "false"}
				}
			}
			return response, nil
		}
		// use put write to record every key with ts
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			Kind:    lock.Kind,
			StartTS: req.StartVersion, // the version we need to find in default cf
		})
		//after we commit we need to delete that lock
		txn.DeleteLock(key)
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			response.RegionError = regionErr.RequestErr
			return response, nil
		}
		return nil, err
	}
	return response, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
