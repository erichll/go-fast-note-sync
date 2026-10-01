package sync

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

var replaceFile = replaceFileImpl

// Create the commit candidate in the destination directory so Windows inherits
// that directory's ACL, rather than retaining the temp-chunks directory ACL.
// Chunk assembly and validation still happen outside the vault.
func stageFileInDestDir(source, destination string) (staged string, err error) {
	mode := os.FileMode(0o644)
	if info, statErr := os.Stat(destination); statErr == nil {
		mode = info.Mode().Perm()
	} else if !os.IsNotExist(statErr) {
		return "", fmt.Errorf("stat destination: %w", statErr)
	}
	in, err := os.Open(source)
	if err != nil {
		return "", fmt.Errorf("open merged file: %w", err)
	}
	defer in.Close()
	out, err := os.CreateTemp(filepath.Dir(destination), "."+filepath.Base(destination)+".tmp-*")
	if err != nil {
		return "", fmt.Errorf("create destination staging: %w", err)
	}
	staged = out.Name()
	defer func() {
		if err != nil {
			_ = out.Close()
			_ = os.Remove(staged)
			staged = ""
		}
	}()
	if _, err = io.Copy(out, in); err != nil {
		return staged, fmt.Errorf("copy destination staging: %w", err)
	}
	if err = out.Chmod(mode); err != nil {
		return staged, fmt.Errorf("chmod destination staging: %w", err)
	}
	if err = out.Sync(); err != nil {
		return staged, fmt.Errorf("sync destination staging: %w", err)
	}
	if err = out.Close(); err != nil {
		return staged, fmt.Errorf("close destination staging: %w", err)
	}
	return staged, nil
}

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
