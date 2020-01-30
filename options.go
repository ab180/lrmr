package lrmr

type Option func(op *Options)

type Options struct {
	OutputChannelSize int `json:"outputBufferSize"`
	OutputBufferSize  int `json:"outputBufferSize"`

	AdvertisedHost string
}

func DefaultOptions() *Options {
	return &Options{
		OutputChannelSize: 1000,
		OutputBufferSize:  1000,
	}
}
