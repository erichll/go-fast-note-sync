//go:build !windows

package sync

import "os"

func replaceFileImpl(source, destination string) error {
	return os.Rename(source, destination)
}
