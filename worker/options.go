package worker

type Options struct {
	Bind string
	Host string
	Port int
}

func DefaultOptions() *Options {
	return &Options{
		Bind: "0.0.0.0",
		Host: "127.0.0.1:7466",
		Port: 7466,
	}
}
