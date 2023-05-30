//go:build mage

package main

import (
	"fmt"
	"os"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

type Lint mg.Namespace

// Run all linters
func (l Lint) All() error {
	mg.Deps(l.Go, l.Extra)
	return nil
}

// Lint everything that's not code
func (l Lint) Extra() error {
	mg.Deps(l.Markdown, l.Yaml)
	return nil
}

// Lint yaml
// TODO: dir
func (Lint) Yaml() error {
	mg.Deps(checkDocker)
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	return sh.RunV("docker", "run", "--rm",
		"-v", fmt.Sprintf("%s:/src:ro", cwd),
		"cytopia/yamllint:1", "-c", "/src/.yamllint", "/src")
}

// Lint markdown
// TODO: dir
func (Lint) Markdown() error {
	mg.Deps(checkDocker)
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	return sh.RunV("docker", "run", "--rm",
		"-v", fmt.Sprintf("%s:/src:ro", cwd),
		"ghcr.io/igorshubovych/markdownlint-cli:v0.34.0", "--config", "/src/.markdownlint.yaml", "/src")
}

// Run all go linters
func (l Lint) Go() error {
	mg.Deps(l.Gofumpt, l.Golangcilint, l.Vulncheck)
	return nil
}

// Run gofumpt
func (Lint) Gofumpt() error {
	fmt.Println("formatting go")
	return runDirV("magefiles", "go", "run", "mvdan.cc/gofumpt", "-l", "-w", "..")
}

// Run golangci-lint
func (Lint) Golangcilint() error {
	fmt.Println("running golangci-lint")
	return runDirV("magefiles", "go", "run", "github.com/golangci/golangci-lint/cmd/golangci-lint", "run", "--fix", "../...")
}

// Run vulncheck
func (Lint) Vulncheck() error {
	fmt.Println("running vulncheck")
	return runDirV("magefiles", "go", "run", "golang.org/x/vuln/cmd/govulncheck", "../...")
}
