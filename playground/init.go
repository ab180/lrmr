package playground

import "github.com/therne/lrmr/transformation"

func init() {
	transformation.Register(&ndjsonDecoder{})
	transformation.Register(&Counter{})
}
