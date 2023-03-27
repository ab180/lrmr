package integration

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ab180/lrmr/coordinator"
	"github.com/rs/zerolog/log"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/thoas/go-funk"
)

const (
	etcdEndpointEnvKey  = "LRMR_TEST_ETCD_ENDPOINT"
	defaultEtcdEndpoint = "127.0.0.1:2379"
)

// ProvideEtcd provides coordinator.Etcd on integration tests.
// Otherwise, coordinator.LocalMemory is provided.
func ProvideEtcd() (crd coordinator.Coordinator, closer func()) {
	if !IsIntegrationTest {
		return coordinator.NewLocalMemory(), func() {}
	}
	testNs := fmt.Sprintf("lrmr_test_%s/", funk.RandomString(10))

	etcdEndpoint, ok := os.LookupEnv(etcdEndpointEnvKey)
	if !ok {
		etcdEndpoint = defaultEtcdEndpoint
	}
	etcd, err := coordinator.NewEtcd([]string{etcdEndpoint}, testNs)
	if err != nil {
		So(err, ShouldBeNil)
	}

	// clean all items under test namespace
	closer = func() {
		time.Sleep(400 * time.Millisecond)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		log.Info().Msg("Closing etcd")
		if _, err := etcd.Delete(ctx, ""); err != nil {
			So(err, ShouldBeNil)
		}
		if err := etcd.Close(); err != nil {
			So(err, ShouldBeNil)
		}
	}
	return etcd, closer
}
