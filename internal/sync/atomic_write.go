package sync

import (
	"fmt"
	"os"
	"path/filepath"
)

var replaceFile = replaceFileImpl

func atomicWriteFile(destination string, content []byte) (err error) {
	mode := os.FileMode(0o644)
	if info, statErr := os.Stat(destination); statErr == nil {
		mode = info.Mode().Perm()
	} else if !os.IsNotExist(statErr) {
		return fmt.Errorf("stat destination: %w", statErr)
	}

	temp, err := os.CreateTemp(filepath.Dir(destination), "."+filepath.Base(destination)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temporary file: %w", err)
	}
	tempPath := temp.Name()
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tempPath)
		}
		if err != nil {
			err = fmt.Errorf("write %q: %w", destination, err)
		}
	}()

	if err := temp.Chmod(mode); err != nil {
		_ = temp.Close()
		return err
	}
	if _, err := temp.Write(content); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := replaceFile(tempPath, destination); err != nil {
		return err
	}
	committed = true
	return nil
}
