//go:build mage

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

var Aliases = map[string]interface{}{
	"test": Test.Unit,
	"lint": Lint.All,
}

type Test mg.Namespace

// Runs the unit tests
func (Test) Unit() error {
	fmt.Println("running unit tests")
	return goTest("./...", "-timeout", "10m")
}

// Runs all integration tests (multi-node cluster tests)
func (Test) Integration() error {
	if err := checkDocker(); err != nil {
		return fmt.Errorf("docker check failed: %w", err)
	}

	fmt.Println("running multi-node integration tests with testcontainers")
	fmt.Println("this creates 3-node CockroachDB clusters and may take several minutes...")
	return goTestWithTags("./...", "integration", "-timeout", "25m", "-v")
}

// Runs cluster health tests (faster subset)
func (Test) IntegrationHealth() error {
	if err := checkDocker(); err != nil {
		return fmt.Errorf("docker check failed: %w", err)
	}

	fmt.Println("running multi-node cluster health tests (faster)")
	return goTestWithTags("./...", "integration", "-run", "TestMultiNodeClusterHealth", "-timeout", "15m", "-v")
}

// Runs a specific integration test by pattern
func (Test) IntegrationRun(pattern string) error {
	if err := checkDocker(); err != nil {
		return fmt.Errorf("docker check failed: %w", err)
	}

	if pattern == "" {
		return fmt.Errorf("test pattern cannot be empty")
	}

	fmt.Printf("running specific integration test pattern: %s\n", pattern)
	return goTestWithTags("./...", "integration", "-run", pattern, "-timeout", "15m", "-v")
}

// Runs pool functionality tests with node failures
func (Test) IntegrationFailure() error {
	if err := checkDocker(); err != nil {
		return fmt.Errorf("docker check failed: %w", err)
	}

	fmt.Println("running multi-node pool failure tests")
	return goTestWithTags("./...", "integration", "-run", "TestMultiNodePoolFunctionality", "-timeout", "20m", "-v")
}

// Runs all tests (unit + integration)
func (Test) All() error {
	fmt.Println("running all tests (unit + integration)")

	t := Test{}
	if err := t.Unit(); err != nil {
		return fmt.Errorf("unit tests failed: %w", err)
	}

	if err := t.Integration(); err != nil {
		return fmt.Errorf("integration tests failed: %w", err)
	}

	fmt.Println("✅ all tests passed!")
	return nil
}

// Cleans up any leftover Docker containers from tests
func (Test) CleanupContainers() error {
	if err := checkDocker(); err != nil {
		return fmt.Errorf("docker check failed: %w", err)
	}

	fmt.Println("cleaning up any leftover test containers...")

	// Find and remove testcontainers containers
	cmd := exec.Command("docker", "ps", "-a", "--filter", "label=org.testcontainers=true", "-q")
	output, err := cmd.Output()
	if err == nil && len(strings.TrimSpace(string(output))) > 0 {
		containerIDs := strings.Fields(string(output))
		if len(containerIDs) > 0 {
			fmt.Printf("found %d leftover testcontainers, cleaning up...\n", len(containerIDs))
			removeArgs := append([]string{"rm", "-f"}, containerIDs...)
			if err := sh.Run("docker", removeArgs...); err != nil {
				fmt.Printf("warning: failed to remove some containers: %v\n", err)
			}
		}
	}

	// Clean up networks
	cmd = exec.Command("docker", "network", "ls", "--filter", "name=testcontainers", "-q")
	output, err = cmd.Output()
	if err == nil && len(strings.TrimSpace(string(output))) > 0 {
		networkIDs := strings.Fields(string(output))
		if len(networkIDs) > 0 {
			fmt.Printf("found %d leftover testcontainer networks, cleaning up...\n", len(networkIDs))
			removeArgs := append([]string{"network", "rm"}, networkIDs...)
			if err := sh.Run("docker", removeArgs...); err != nil {
				fmt.Printf("warning: failed to remove some networks: %v\n", err)
			}
		}
	}

	fmt.Println("cleanup completed")
	return nil
}

// Checks Docker installation and status
func (Test) CheckDocker() error {
	if err := checkDocker(); err != nil {
		fmt.Printf("❌ Docker check failed: %v\n", err)
		return err
	}
	fmt.Println("✅ Docker is available and running")
	return nil
}

// run go test in the root
func goTest(path string, args ...string) error {
	return goDirTest(".", path, args...)
}

// run go test in a directory
func goDirTest(dir string, path string, args ...string) error {
	testArgs := append([]string{"test", "-failfast", "-count=1"}, args...)
	testArgs = append(testArgs, path)
	return runDirV(dir, goCmdForTests(), testArgs...)
}

// run go test with build tags
func goTestWithTags(path string, tags string, args ...string) error {
	testArgs := append([]string{"test", "-failfast", "-count=1", "-tags", tags}, args...)
	testArgs = append(testArgs, path)
	return runDirV(".", goCmdForTests(), testArgs...)
}

// run a command in a directory
func runDirV(dir string, cmd string, args ...string) error {
	c := exec.Command(cmd, args...)
	c.Dir = dir
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	return c.Run()
}

// check if docker is installed and running
func checkDocker() error {
	if !hasBinary("docker") {
		return fmt.Errorf("docker must be installed to run integration tests")
	}

	// Check if Docker daemon is running with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", "info")
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker is not running or not accessible: %w", err)
	}

	return nil
}

// check if a binary exists
func hasBinary(binaryName string) bool {
	_, err := exec.LookPath(binaryName)
	return err == nil
}

// use `richgo` for running tests if it's available
func goCmdForTests() string {
	if hasBinary("richgo") {
		return "richgo"
	}
	return "go"
}
