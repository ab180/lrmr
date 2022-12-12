package lrmrpb

func (req *PushDataRequest) RemainCapicityReset() {
	for _, row := range req.Data {
		row.RemainCapicityReset()
	}

	data := req.Data[:0]
	req.Reset()
	req.Data = data
}
