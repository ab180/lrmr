module github.com/therne/lrmr

go 1.14

require (
	github.com/airbloc/logger v1.4.5
	github.com/creasty/defaults v1.3.0
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.3.5
	github.com/goombaio/namegenerator v0.0.0-20181006234301-989e774b106e
	github.com/grpc-ecosystem/go-grpc-middleware v1.2.0
	github.com/jinzhu/copier v0.0.0-20190924061706-b57f9002281a
	github.com/json-iterator/go v1.1.9
	github.com/maruel/panicparse v1.5.0 // indirect
	github.com/modern-go/reflect2 v1.0.1
	github.com/pkg/errors v0.9.1
	github.com/segmentio/fasthash v1.0.1
	github.com/smartystreets/goconvey v1.6.4
	github.com/thoas/go-funk v0.5.0
	github.com/vmihailenco/msgpack/v5 v5.0.0-beta.1
	go.etcd.io/etcd/api/v3 v3.0.0-20201026174226-7da5182f1d02
	go.etcd.io/etcd/client/v3 v3.0.0-20201026174226-7da5182f1d02
	go.uber.org/atomic v1.6.0
	go.uber.org/goleak v1.1.10
	go.uber.org/zap v1.16.0
	golang.org/x/sync v0.0.0-20201020160332-67f06af15bc9
	google.golang.org/grpc v1.29.1
)

replace (
	go.etcd.io/etcd/api/v3 => go.etcd.io/etcd/api/v3 v3.0.0-20201026174226-7da5182f1d02
	go.etcd.io/etcd/pkg/v3 => go.etcd.io/etcd/pkg/v3 v3.0.0-20201026174226-7da5182f1d02
	golang.org/x/lint => golang.org/x/lint v0.0.0-20200302205851-738671d3881b
	golang.org/x/net => github.com/golang/net v0.0.0-20201027133719-8eef5233e2a1
	golang.org/x/sync => github.com/golang/sync v0.0.0-20201020160332-67f06af15bc9
	golang.org/x/sys => github.com/golang/sys v0.0.0-20201027140754-0fcbb8f4928c
	golang.org/x/text => github.com/golang/text v0.3.4
	golang.org/x/tools => golang.org/x/tools v0.0.0-20201028025901-8cd080b735b3
	golang.org/x/xerrors => golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1
)
