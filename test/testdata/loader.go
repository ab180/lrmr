package testdata

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"

	"github.com/pkg/errors"
)

const (
	EnvKeyTestdataDir = "LRMR_TESTDATA_DIR"
	errNoPathFound    = "Please specify test data directory with LRMR_TESTDATA_DIR env."
)

var testDataSearchPaths = []string{"./test/testdata", "./testdata"}

func Path() string {
	envPath, exists := os.LookupEnv(EnvKeyTestdataDir)
	if exists {
		return envPath
	}
	for _, base := range testDataSearchPaths {
		if err := checkDirectory(base); err != nil {
			continue
		}
		// extract if the data does not exist
		unpackDir := filepath.Join(base, "unpacked") + "/"
		if err := checkDirectory(unpackDir); err != nil {
			fmt.Println("Extracting test data...")
			if err := runCmd("mkdir", "-p", unpackDir); err != nil {
				panic(errors.Wrap(err, "failed to mkdir "+unpackDir))
			}
			if err := runCmd("tar", "-xvf", path.Join(base, Name), "-C", unpackDir); err != nil {
				panic(errors.Wrap(err, "failed to unpack data"))
			}
			fmt.Println("  -> Done!")
		}
		dir, _ := filepath.Abs(unpackDir)
		return dir
	}
	panic("testdata not found. " + errNoPathFound)
}

func checkDirectory(path string) error {
	stat, err := os.Stat(path)
	if os.IsNotExist(err) {
		return errors.Errorf("%s does not exist. %s", path, errNoPathFound)
	} else if !stat.IsDir() {
		return errors.Errorf("%s is not a directory. %s", path, errNoPathFound)
	}
	return nil
}

func runCmd(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return errors.Errorf("%s: %s", err, string(out))
	}
	return nil
}
