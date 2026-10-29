package common

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
)

func ReadToml[T any](tomlPath string) (T, error) {
	var model T
	meta, err := toml.DecodeFile(tomlPath, &model)
	if err != nil {
		return model, err
	}
	// Unknown keys are contradictions between file and schema; reject them
	// at parse time instead of silently ignoring them (AUD-006).
	if undecoded := meta.Undecoded(); len(undecoded) > 0 {
		keys := make([]string, 0, len(undecoded))
		for _, k := range undecoded {
			keys = append(keys, k.String())
		}
		return model, fmt.Errorf("%s: unknown configuration keys: %s", tomlPath, strings.Join(keys, ", "))
	}
	return model, nil
}

func CreateTar(path string) ([]byte, error) {
	var buffer bytes.Buffer
	tw := tar.NewWriter(&buffer)

	err := filepath.WalkDir(path, func(file string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		relPath, err := filepath.Rel(path, file)
		if err != nil {
			return err
		}

		// FileInfoHeader's second argument is the link target; an empty one
		// makes every symlink arrive broken (AUD-029).
		linkname := ""
		if info.Mode()&fs.ModeSymlink != 0 {
			linkname, err = os.Readlink(file)
			if err != nil {
				return err
			}
		}
		header, err := tar.FileInfoHeader(info, linkname)
		if err != nil {
			return err
		}
		header.Name = relPath
		if info.IsDir() {
			header.Name += "/"
		}
		if err := tw.WriteHeader(header); err != nil {
			return err
		}

		if info.Mode().IsRegular() {
			f, err := os.Open(file)
			if err != nil {
				return err
			}
			_, err = io.Copy(tw, f)
			closeErr := f.Close()
			if err != nil {
				return err
			}
			if closeErr != nil {
				return closeErr
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Close flushes the final padding and the end-of-archive blocks, so it
	// must run before the bytes are read.
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}
