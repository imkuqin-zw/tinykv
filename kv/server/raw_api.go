package server

import (
	"context"
	"errors"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(ctx context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	data, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return &kvrpcpb.RawGetResponse{
			RegionError: nil,
			Error:       err.Error(),
			Value:       data,
			NotFound:    true,
		}, nil
		//return nil, err
	}
	result := &kvrpcpb.RawGetResponse{Value: data}
	if data == nil {
		result.NotFound = true
	}
	return result, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}
	if err := server.storage.Write(req.Context, []storage.Modify{{Data: put}}); err != nil {
		return &kvrpcpb.RawPutResponse{
			Error: err.Error(),
		}, nil
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	del := storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}
	if err := server.storage.Write(req.Context, []storage.Modify{{Data: del}}); err != nil {
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return &kvrpcpb.RawDeleteResponse{
				Error: err.Error(),
			}, nil
		}
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
	//return nil, nil
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
	defer iter.Close()
	result := &kvrpcpb.RawScanResponse{
		RegionError: nil,
		Kvs:         make([]*kvrpcpb.KvPair, 0, req.Limit),
	}
	i := uint32(0)
	iter.Seek(req.StartKey)
	for iter.Valid() {
		item := iter.Item()
		kv := &kvrpcpb.KvPair{
			Key: item.Key(),
		}
		val, err := item.Value()
		if err != nil {

		}
		kv.Value = val
		result.Kvs = append(result.Kvs, kv)
		i++
		if i == req.Limit {
			break
		}
		iter.Next()
	}
	return result, nil
}
