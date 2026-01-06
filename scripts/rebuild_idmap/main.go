package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/vexsearch/vex/internal/config"
	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/pkg/objectstore"
)

var (
	docsColumnMagic   = [4]byte{'V', 'E', 'X', 'D'}
	docsColumnVersion = byte(1)
)

func main() {
	var (
		configPath   string
		namespaceArg string
		force        bool
	)

	flag.StringVar(&configPath, "config", "", "Path to vex config file")
	flag.StringVar(&namespaceArg, "namespace", "", "Namespace to rebuild id maps for")
	flag.BoolVar(&force, "force", false, "Overwrite id maps even if they already exist")
	flag.Parse()

	if configPath == "" || namespaceArg == "" {
		fmt.Fprintln(os.Stderr, "Usage: rebuild_idmap -config <path> -namespace <ns> [-force]")
		os.Exit(2)
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	store, err := objectstore.New(objectstore.Config{
		Type:      cfg.ObjectStore.Type,
		Endpoint:  cfg.ObjectStore.Endpoint,
		Bucket:    cfg.ObjectStore.Bucket,
		AccessKey: cfg.ObjectStore.AccessKey,
		SecretKey: cfg.ObjectStore.SecretKey,
		Region:    cfg.ObjectStore.Region,
		UseSSL:    cfg.ObjectStore.UseSSL,
		RootPath:  cfg.ObjectStore.RootPath,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to init object store: %v\n", err)
		os.Exit(1)
	}

	ctx := context.Background()
	stateMan := namespace.NewStateManager(store)
	loaded, err := stateMan.Load(ctx, namespaceArg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load state: %v\n", err)
		os.Exit(1)
	}

	if loaded.State.Index.ManifestKey == "" {
		fmt.Fprintln(os.Stderr, "namespace has no manifest; aborting")
		os.Exit(1)
	}

	reader := index.NewReader(store, nil, nil)
	manifest, err := reader.LoadManifest(ctx, loaded.State.Index.ManifestKey)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load manifest: %v\n", err)
		os.Exit(1)
	}
	if manifest == nil {
		fmt.Fprintln(os.Stderr, "manifest not found; aborting")
		os.Exit(1)
	}

	for _, seg := range manifest.Segments {
		if seg.DocsKey == "" {
			continue
		}
		idMapKey := index.DocsIDMapKey(seg.DocsKey)
		if idMapKey == "" {
			continue
		}

		if !force {
			if _, err := store.Head(ctx, idMapKey); err == nil {
				fmt.Printf("id map already exists: %s\n", idMapKey)
				continue
			} else if !objectstore.IsNotFoundError(err) {
				fmt.Fprintf(os.Stderr, "failed to check id map %s: %v\n", idMapKey, err)
				os.Exit(1)
			}
		}

		start := time.Now()
		ids, err := buildDocIDMap(ctx, store, seg.DocsKey, int(seg.Stats.RowCount))
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to build id map for %s: %v\n", seg.DocsKey, err)
			os.Exit(1)
		}
		if len(ids) == 0 {
			fmt.Printf("skipping id map for %s (no docs)\n", seg.DocsKey)
			continue
		}

		data, err := index.EncodeDocIDMap(ids)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to encode id map for %s: %v\n", seg.DocsKey, err)
			os.Exit(1)
		}

		putFn := store.PutIfAbsent
		if force {
			putFn = store.Put
		}
		if _, err := putFn(ctx, idMapKey, bytes.NewReader(data), int64(len(data)), &objectstore.PutOptions{
			ContentType: "application/octet-stream",
		}); err != nil {
			fmt.Fprintf(os.Stderr, "failed to write id map for %s: %v\n", seg.DocsKey, err)
			os.Exit(1)
		}

		fmt.Printf("wrote id map: segment=%s docs=%d bytes=%d dur=%s\n",
			seg.ID, len(ids), len(data), time.Since(start))
	}
}

func buildDocIDMap(ctx context.Context, store objectstore.Store, docsKey string, expected int) ([]uint64, error) {
	reader, _, err := store.Get(ctx, docsKey, nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	magic := make([]byte, 4)
	n, err := io.ReadFull(reader, magic)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, fmt.Errorf("failed to read docs magic: %w", err)
	}
	if n == 0 {
		return nil, nil
	}

	fullReader := io.MultiReader(bytes.NewReader(magic[:n]), reader)
	if n >= 4 && magic[0] == 0x28 && magic[1] == 0xB5 && magic[2] == 0x2F && magic[3] == 0xFD {
		dec, err := zstd.NewReader(fullReader,
			zstd.WithDecoderLowmem(true),
			zstd.WithDecoderMaxWindow(32*1024*1024),
			zstd.WithDecoderConcurrency(1),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd reader: %w", err)
		}
		defer dec.Close()
		fullReader = dec
	}

	buffered := bufio.NewReader(fullReader)
	peek, err := buffered.Peek(len(docsColumnMagic))
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, fmt.Errorf("failed to peek docs data: %w", err)
	}
	if len(peek) == len(docsColumnMagic) && bytes.Equal(peek, docsColumnMagic[:]) {
		docCount, err := readDocsColumnHeader(buffered)
		if err != nil {
			return nil, err
		}
		if docCount == 0 {
			return nil, nil
		}
		ids := make([]uint64, docCount)
		if err := binary.Read(buffered, binary.LittleEndian, ids); err != nil {
			return nil, fmt.Errorf("failed to read numeric IDs: %w", err)
		}
		return ids, nil
	}

	decoder := json.NewDecoder(buffered)
	token, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("failed to read JSON start: %w", err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '[' {
		return nil, fmt.Errorf("expected JSON array, got %v", token)
	}

	capacity := expected
	if capacity <= 0 {
		capacity = 1024
	}
	ids := make([]uint64, 0, capacity)

	for decoder.More() {
		if ctx != nil && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var partial struct {
			NumericID uint64 `json:"NumericID"`
		}
		if err := decoder.Decode(&partial); err != nil {
			return nil, fmt.Errorf("failed to decode doc ID: %w", err)
		}
		ids = append(ids, partial.NumericID)
	}

	return ids, nil
}

func readDocsColumnHeader(r io.Reader) (int, error) {
	var magic [4]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return 0, fmt.Errorf("failed to read docs magic: %w", err)
	}
	if magic != docsColumnMagic {
		return 0, fmt.Errorf("invalid docs column magic")
	}
	version, err := readByte(r)
	if err != nil {
		return 0, fmt.Errorf("failed to read docs version: %w", err)
	}
	if version != docsColumnVersion {
		return 0, fmt.Errorf("unsupported docs column version %d", version)
	}
	if _, err := readByte(r); err != nil {
		return 0, fmt.Errorf("failed to read docs flags: %w", err)
	}
	if _, err := readUint16(r); err != nil {
		return 0, fmt.Errorf("failed to read docs padding: %w", err)
	}
	docCount, err := readUint64(r)
	if err != nil {
		return 0, fmt.Errorf("failed to read docs count: %w", err)
	}
	if _, err := readUint32(r); err != nil {
		return 0, fmt.Errorf("failed to read vector dims: %w", err)
	}
	if docCount > uint64(int(^uint(0)>>1)) {
		return 0, fmt.Errorf("docs column too large: %d", docCount)
	}
	return int(docCount), nil
}

func readByte(r io.Reader) (byte, error) {
	var b [1]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return b[0], nil
}

func readUint16(r io.Reader) (uint16, error) {
	var b [2]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(b[:]), nil
}

func readUint32(r io.Reader) (uint32, error) {
	var b [4]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b[:]), nil
}

func readUint64(r io.Reader) (uint64, error) {
	var b [8]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b[:]), nil
}
