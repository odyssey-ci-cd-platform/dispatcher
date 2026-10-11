package common_test

import (
	"archive/tar"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/odyssey-ci-cd-platform/dispatcher/internal/common"
)

func TestCreateTar(t *testing.T) {
	t.Run("archives regular files with content", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("hello world"), 0o644); err != nil {
			t.Fatal(err)
		}

		got, err := common.CreateTar(dir)
		if err != nil {
			t.Fatalf("CreateTar() unexpected error: %v", err)
		}
		if len(got) == 0 {
			t.Fatal("CreateTar() returned an empty archive")
		}

		names := readTarNames(t, got)
		if !slices.Contains(names, "hello.txt") {
			t.Errorf("archive missing hello.txt; entries: %v", names)
		}
	})

	t.Run("empty directory still produces an archive", func(t *testing.T) {
		got, err := common.CreateTar(t.TempDir())
		if err != nil {
			t.Fatalf("CreateTar() unexpected error: %v", err)
		}
		if len(got) == 0 {
			t.Fatal("CreateTar() of an empty directory returned an empty archive")
		}
	})

	t.Run("archive is a valid tar with end-of-archive blocks", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}

		got, err := common.CreateTar(dir)
		if err != nil {
			t.Fatalf("CreateTar() unexpected error: %v", err)
		}
		// A well-formed tar ends with two 512-byte zero blocks, so its length
		// is always a multiple of the 512-byte block size.
		if len(got)%512 != 0 {
			t.Errorf("archive length = %d, want a multiple of 512 (end-of-archive blocks missing)", len(got))
		}
	})

	t.Run("missing path returns error", func(t *testing.T) {
		_, err := common.CreateTar(filepath.Join(t.TempDir(), "does-not-exist"))
		if err == nil {
			t.Fatal("CreateTar() expected error for a missing path, got nil")
		}
	})
}

// readTarNames returns the names of all entries in a tar archive.
func readTarNames(t *testing.T, archive []byte) []string {
	t.Helper()
	tr := tar.NewReader(bytes.NewReader(archive))
	var names []string
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("reading archive: %v", err)
		}
		names = append(names, hdr.Name)
	}
	return names
}
