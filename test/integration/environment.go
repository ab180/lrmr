package integration

import (
	"os"
	"testing"

	"github.com/airbloc/logger"
)

const envKey = "LRMR_TEST_INTEGRATION"

var log = logger.New("test")

// IsIntegrationTest indicates that current test is an integration test.
// will be turned on if LRMR_TEST_INTEGRATION environment variable is set.
var IsIntegrationTest bool

func init() {
	if _, ok := os.LookupEnv(envKey); ok {
		IsIntegrationTest = true
		log.Info("Starting integration test.")
	}
}

func RunOnIntegrationTest(t *testing.T) {
	if !IsIntegrationTest {
		t.Skipf("Skipping %s since it is integration test.", t.Name())
	}
}
