package testdata

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/pkg/errors"
)

const (
	EnvKeyTestdataDir = "LRMR_TESTDATA_DIR"
	errNoPathFound    = "Please make sure that pwd is the project root directory, or specify test data directory with LRMR_TESTDATA_DIR env."
)

func Path() string {
	envPath, exists := os.LookupEnv(EnvKeyTestdataDir)
	if exists {
		return envPath
	}
	// assume that `pwd` is the project root
	if err := checkDirectory("./test/testdata"); err != nil {
		panic(err)
	}
	// extract if the data does not exist
	if err := checkDirectory("./test/testdata/unpacked"); err != nil {
		fmt.Println("Extracting test data...")
		if err := runCmd("mkdir", "-p", "./test/testdata/unpacked/"); err != nil {
			panic(errors.Wrap(err, "failed to mkdir ./test/testdata/unpacked/"))
		}
		if err := runCmd("tar", "-xvf", "./test/testdata/"+Name, "-C", "./test/testdata/unpacked/"); err != nil {
			panic(errors.Wrap(err, "failed to unpack data"))
		}
		fmt.Println("  -> Done!")
	}
	dir, _ := filepath.Abs("./test/testdata/unpacked/")
	return dir
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
