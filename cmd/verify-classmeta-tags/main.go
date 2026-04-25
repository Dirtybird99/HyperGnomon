// verify-classmeta-tags opens a bbolt DB read-only and reports the
// on-disk tag-byte distribution for the class_scid bucket, plus a
// round-trip sample confirming UnmarshalTyped decodes real records.
//
// Usage: verify-classmeta-tags <db-dir>
package main

import (
	"fmt"
	"os"
	"path/filepath"

	bolt "go.etcd.io/bbolt"

	"github.com/hypergnomon/hypergnomon/structures"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: verify-classmeta-tags <db-dir>")
		os.Exit(2)
	}
	dbPath := filepath.Join(os.Args[1], "HYPERGNOMON.db")
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		fmt.Fprintf(os.Stderr, "open %s: %v\n", dbPath, err)
		os.Exit(1)
	}
	defer db.Close()

	tagCounts := make(map[byte]int)
	classCounts := make(map[string]int)
	var total, typedOK, firstErr int
	var sampleScid string
	var sampleMeta structures.ClassMeta
	var telaIndexScid string
	var telaIndexMeta structures.ClassMeta
	var firstErrMsg string

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("class_scid"))
		if b == nil {
			return fmt.Errorf("class_scid bucket missing")
		}
		return b.ForEach(func(k, v []byte) error {
			total++
			if len(v) == 0 {
				return nil
			}
			tagCounts[v[0]]++
			if structures.IsClassMetaTyped(v) {
				var m structures.ClassMeta
				if err := m.UnmarshalTyped(v); err != nil {
					firstErr++
					if firstErrMsg == "" {
						firstErrMsg = fmt.Sprintf("%s: %v", string(k), err)
					}
					return nil
				}
				typedOK++
				classCounts[m.Class]++
				if sampleScid == "" {
					sampleScid = string(k)
					sampleMeta = m
				}
				if telaIndexScid == "" && m.Class == "TELA-INDEX-1" {
					telaIndexScid = string(k)
					telaIndexMeta = m
				}
			}
			return nil
		})
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "view: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("class_scid bucket\n")
	fmt.Printf("  total records: %d\n", total)
	fmt.Printf("  tag byte distribution:\n")
	for tag, n := range tagCounts {
		label := ""
		switch {
		case tag == 0x04:
			label = "typed v1 (ClassMeta)"
		case tag >= 0x80 && tag <= 0x8f:
			label = "msgpack fixmap (legacy)"
		case tag == 0xde:
			label = "msgpack map16 (legacy)"
		default:
			label = "unknown"
		}
		fmt.Printf("    0x%02x  %6d  (%s)\n", tag, n, label)
	}
	fmt.Printf("  typed decode successes: %d / typed records\n", typedOK)
	if firstErr > 0 {
		fmt.Printf("  typed decode FAILURES: %d — first: %s\n", firstErr, firstErrMsg)
	}
	if len(classCounts) > 0 {
		fmt.Printf("\nclass distribution\n")
		for cls, n := range classCounts {
			fmt.Printf("  %-16s %d\n", cls, n)
		}
	}
	if telaIndexScid != "" {
		fmt.Printf("\nsample TELA-INDEX-1 record\n")
		fmt.Printf("  scid: %s\n", telaIndexScid)
		fmt.Printf("  class: %s\n", telaIndexMeta.Class)
		fmt.Printf("  tags: %v\n", telaIndexMeta.Tags)
		fmt.Printf("  name: %q\n", telaIndexMeta.Name)
		fmt.Printf("  desc: %q\n", telaIndexMeta.Desc)
		fmt.Printf("  icon: %q\n", telaIndexMeta.IconURL)
		fmt.Printf("  durl: %q  version: %q\n", telaIndexMeta.DURL, telaIndexMeta.Version)
		fmt.Printf("  install_h: %d  last_h: %d\n", telaIndexMeta.InstallHeight, telaIndexMeta.LastHeight)
	}
	if sampleScid != "" {
		fmt.Printf("\nsample typed record\n")
		fmt.Printf("  scid: %s\n", sampleScid)
		fmt.Printf("  class: %s\n", sampleMeta.Class)
		fmt.Printf("  tags: %v\n", sampleMeta.Tags)
		fmt.Printf("  name: %q\n", sampleMeta.Name)
		fmt.Printf("  durl: %q  version: %q\n", sampleMeta.DURL, sampleMeta.Version)
		fmt.Printf("  install_h: %d  last_h: %d\n", sampleMeta.InstallHeight, sampleMeta.LastHeight)
	}
}
