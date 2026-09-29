package sync

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

var replaceFile = replaceFileImpl

// stageFileInDestDir copies a verified staged file into the destination's own
// directory so the final replaceFile renames within that directory and the
// committed file inherits the destination folder's ACL — a same-volume rename
// from elsewhere keeps the source folder's ACL, which locks SMB users out.
func stageFileInDestDir(staged, destination string) (string, error) {
	mode := os.FileMode(0o644)
	if info, statErr := os.Stat(destination); statErr == nil {
		mode = info.Mode().Perm()
	} else if !os.IsNotExist(statErr) {
		return "", fmt.Errorf("stat destination: %w", statErr)
	}
	tmp, err := os.CreateTemp(filepath.Dir(destination), "."+filepath.Base(destination)+".tmp-*")
	if err != nil {
		return "", fmt.Errorf("create temporary file: %w", err)
	}
	tmpPath := tmp.Name()
	in, err := os.Open(staged)
	if err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("open staged file: %w", err)
	}
	_, copyErr := io.Copy(tmp, in)
	closeErr := in.Close()
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("chmod temporary file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("write temporary file: %w", err)
	}
	if copyErr != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("copy staged data: %w", copyErr)
	}
	if closeErr != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("close staged file: %w", closeErr)
	}
	return tmpPath, nil
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
