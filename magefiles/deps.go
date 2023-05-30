//go:build mage

package main

import "github.com/magefile/mage/mg"

var goModules = []string{
	".", "magefiles",
}

type Deps mg.Namespace

// go mod tidy all go modules
func (Deps) Tidy() error {
	for _, mod := range goModules {
		if err := runDirV(mod, "go", "mod", "tidy"); err != nil {
			return err
		}
	}

	return nil
}

// go get -u all go dependencies
func (Deps) Update() error {
	for _, mod := range goModules {
		if err := runDirV(mod, "go", "get", "-u", "-t", "-tags", "mage", "./..."); err != nil {
			return err
		}

		if err := runDirV(mod, "go", "mod", "tidy"); err != nil {
			return err
		}
	}

	return nil
}
