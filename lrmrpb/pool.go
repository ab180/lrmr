package lrmrpb

import (
	sync "sync"

	lrdd "github.com/ab180/lrmr/lrdd"
)

func GetPushDataRequest(dataSize int) *PushDataRequest {
	req := pushDataRequestPool.Get().(*PushDataRequest)

	reqDataCap := cap(req.Data)
	if dataSize <= reqDataCap {
		req.Data = req.Data[:dataSize]
		for i := range req.Data {
			if req.Data[i] != nil {
				continue
			} else {
				req.Data[i] = &lrdd.RawRow{}
			}
		}
	} else {
		data := req.Data[:reqDataCap]
		req.Data = make([]*lrdd.RawRow, dataSize)
		for i := range req.Data {
			if i < reqDataCap {
				if data[i] != nil {
					req.Data[i] = data[i]
				} else {
					req.Data[i] = &lrdd.RawRow{}
				}
			} else {
				req.Data[i] = &lrdd.RawRow{}
			}
		}
	}

	return req
}

func PutPushDataRequest(req *PushDataRequest) {
	for _, row := range req.Data {
		value := row.Value[:0]
		row.Reset()
		row.Value = value
	}

	data := req.Data[:0]
	req.Reset()
	req.Data = data

	pushDataRequestPool.Put(req)
}

var pushDataRequestPool = sync.Pool{
	New: func() any {
		return &PushDataRequest{}
	},
}
