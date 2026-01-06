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

func main() {
	var (
		configPath   string
		namespaceArg string
		force        bool
	)

	flag.StringVar(&configPath, "config", "", "Path to vex config file")
	flag.StringVar(&namespaceArg, "namespace", "", "Namespace to rebuild offsets for")
	flag.BoolVar(&force, "force", false, "Overwrite offsets even if they already exist")
	flag.Parse()

	if configPath == "" || namespaceArg == "" {
		fmt.Fprintln(os.Stderr, "Usage: rebuild_offsets -config <path> -namespace <ns> [-force]")
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
		offsetsKey := index.DocsOffsetsKey(seg.DocsKey)
		if offsetsKey == "" {
			continue
		}

		if !force {
			if _, err := store.Head(ctx, offsetsKey); err == nil {
				fmt.Printf("offsets already exist: %s\n", offsetsKey)
				continue
			} else if !objectstore.IsNotFoundError(err) {
				fmt.Fprintf(os.Stderr, "failed to check offsets %s: %v\n", offsetsKey, err)
				os.Exit(1)
			}
		}

		start := time.Now()
		offsets, err := buildDocOffsets(ctx, store, seg.DocsKey, int(seg.Stats.RowCount))
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to build offsets for %s: %v\n", seg.DocsKey, err)
			os.Exit(1)
		}
		if len(offsets) < 2 {
			fmt.Printf("skipping offsets for %s (no docs)\n", seg.DocsKey)
			continue
		}

		data, err := encodeDocOffsets(offsets)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to encode offsets for %s: %v\n", seg.DocsKey, err)
			os.Exit(1)
		}

		putFn := store.PutIfAbsent
		if force {
			putFn = store.Put
		}
		if _, err := putFn(ctx, offsetsKey, bytes.NewReader(data), int64(len(data)), &objectstore.PutOptions{
			ContentType: "application/octet-stream",
		}); err != nil {
			fmt.Fprintf(os.Stderr, "failed to write offsets for %s: %v\n", offsetsKey, err)
			os.Exit(1)
		}

		fmt.Printf("wrote offsets: segment=%s docs=%d bytes=%d dur=%s\n",
			seg.ID, len(offsets)-1, len(data), time.Since(start))
	}
}

func encodeDocOffsets(offsets []uint64) ([]byte, error) {
	if len(offsets) < 2 {
		return nil, fmt.Errorf("offsets too short")
	}

	var buf bytes.Buffer
	buf.WriteString("DOFF")
	if err := binary.Write(&buf, binary.LittleEndian, uint32(1)); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, uint64(len(offsets)-1)); err != nil {
		return nil, err
	}
	for _, off := range offsets {
		if err := binary.Write(&buf, binary.LittleEndian, off); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func buildDocOffsets(ctx context.Context, store objectstore.Store, docsKey string, expected int) ([]uint64, error) {
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
	peek, err := buffered.Peek(4)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, fmt.Errorf("failed to peek docs data: %w", err)
	}
	if len(peek) == 4 && bytes.Equal(peek, []byte{'V', 'E', 'X', 'D'}) {
		return nil, nil
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
	offsets := make([]uint64, 0, capacity+1)

	for decoder.More() {
		if ctx != nil && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		start := decoder.InputOffset()
		var skip json.RawMessage
		if err := decoder.Decode(&skip); err != nil {
			return nil, fmt.Errorf("failed to decode doc: %w", err)
		}
		offsets = append(offsets, uint64(start))
	}
	end := decoder.InputOffset()
	offsets = append(offsets, uint64(end))

	return offsets, nil
}
