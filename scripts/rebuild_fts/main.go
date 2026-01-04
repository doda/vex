package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/vexsearch/vex/internal/config"
	"github.com/vexsearch/vex/internal/fts"
	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/pkg/objectstore"
)

type segmentDoc struct {
	Deleted    bool           `json:"Deleted"`
	Attributes map[string]any `json:"Attributes"`
}

func main() {
	var (
		configPath   string
		namespaceArg string
		fieldArg     string
		ftsConfigArg string
	)

	flag.StringVar(&configPath, "config", "", "Path to vex config file")
	flag.StringVar(&namespaceArg, "namespace", "", "Namespace to rebuild")
	flag.StringVar(&fieldArg, "field", "", "Comma-separated FTS fields to rebuild")
	flag.StringVar(&ftsConfigArg, "fts-config", "", "Optional FTS config JSON (defaults to true)")
	flag.Parse()

	if configPath == "" || namespaceArg == "" || fieldArg == "" {
		fmt.Fprintln(os.Stderr, "Usage: rebuild_fts -config <path> -namespace <ns> -field <field[,field]> [-fts-config <json>]")
		os.Exit(2)
	}

	fields := splitFields(fieldArg)
	if len(fields) == 0 {
		fmt.Fprintln(os.Stderr, "no fields provided")
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

	if loaded.State.Schema == nil || len(loaded.State.Schema.Attributes) == 0 {
		fmt.Fprintln(os.Stderr, "namespace schema is empty; aborting")
		os.Exit(1)
	}

	ftsConfigRaw := json.RawMessage("true")
	if ftsConfigArg != "" {
		if !json.Valid([]byte(ftsConfigArg)) {
			fmt.Fprintln(os.Stderr, "fts-config must be valid JSON")
			os.Exit(2)
		}
		ftsConfigRaw = json.RawMessage(ftsConfigArg)
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

	for _, field := range fields {
		attr, ok := loaded.State.Schema.Attributes[field]
		if !ok {
			fmt.Fprintf(os.Stderr, "field %q not found in schema\n", field)
			os.Exit(1)
		}
		if strings.ToLower(attr.Type) != "string" {
			fmt.Fprintf(os.Stderr, "field %q is not a string attribute\n", field)
			os.Exit(1)
		}
	}

	updated := manifest.Clone()
	if updated == nil {
		fmt.Fprintln(os.Stderr, "failed to clone manifest")
		os.Exit(1)
	}

	manifestChanged := false
	schemaUpdated := false

	if loaded.State.Schema != nil && loaded.State.Schema.Attributes != nil {
		for _, field := range fields {
			attr := loaded.State.Schema.Attributes[field]
			if len(attr.FullTextSearch) == 0 || !bytes.Equal(attr.FullTextSearch, ftsConfigRaw) {
				schemaUpdated = true
				break
			}
		}
	}

	for segIdx := range updated.Segments {
		seg := &updated.Segments[segIdx]
		if seg.DocsKey == "" || seg.Stats.RowCount == 0 {
			continue
		}
		if err := ensureDocOffsets(ctx, store, seg.DocsKey); err != nil {
			fmt.Fprintf(os.Stderr, "failed to build offsets for %s: %v\n", seg.DocsKey, err)
			os.Exit(1)
		}
		for _, field := range fields {
			if hasFTSKey(seg, field) {
				continue
			}
			fmt.Printf("building FTS index: namespace=%s segment=%s field=%s\n", namespaceArg, seg.ID, field)
			idx, err := buildFTSIndexForSegment(ctx, store, seg.DocsKey, field, ftsConfigRaw)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to build FTS index for %s/%s: %v\n", seg.ID, field, err)
				os.Exit(1)
			}
			if idx == nil || idx.TotalDocs == 0 {
				continue
			}
			data, err := idx.Serialize()
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to serialize FTS index for %s/%s: %v\n", seg.ID, field, err)
				os.Exit(1)
			}
			writer := index.NewSegmentWriter(store, namespaceArg, seg.ID)
			key, err := writer.WriteFTSData(ctx, field, data)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to write FTS index for %s/%s: %v\n", seg.ID, field, err)
				os.Exit(1)
			}
			seg.FTSKeys = append(seg.FTSKeys, key)
			manifestChanged = true
		}
	}

	var newManifestSeq uint64
	newManifestKey := loaded.State.Index.ManifestKey
	if manifestChanged {
		updated.GeneratedAt = time.Now().UTC()
		updated.UpdateIndexedWALSeq()

		newManifestSeq = loaded.State.Index.ManifestSeq + 1
		newManifestKey = index.ManifestKey(namespaceArg, newManifestSeq)
		manifestData, err := updated.MarshalJSON()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to marshal manifest: %v\n", err)
			os.Exit(1)
		}
		_, err = store.PutIfAbsent(ctx, newManifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), &objectstore.PutOptions{
			ContentType: "application/json",
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to write manifest: %v\n", err)
			os.Exit(1)
		}
	}

	if manifestChanged || schemaUpdated {
		_, err = stateMan.Update(ctx, namespaceArg, loaded.ETag, func(state *namespace.State) error {
			if state.Schema == nil {
				state.Schema = &namespace.Schema{Attributes: map[string]namespace.AttributeSchema{}}
			}
			for _, field := range fields {
				attr := state.Schema.Attributes[field]
				attr.FullTextSearch = ftsConfigRaw
				state.Schema.Attributes[field] = attr
			}

			if manifestChanged {
				state.Index.ManifestSeq = newManifestSeq
				state.Index.ManifestKey = newManifestKey
			}
			state.Index.Status = "up-to-date"

			if len(state.Index.PendingRebuilds) > 0 {
				filtered := make([]namespace.PendingRebuild, 0, len(state.Index.PendingRebuilds))
				for _, pr := range state.Index.PendingRebuilds {
					if pr.Kind == "fts" {
						skip := false
						for _, field := range fields {
							if pr.Attribute == field {
								skip = true
								break
							}
						}
						if skip {
							continue
						}
					}
					filtered = append(filtered, pr)
				}
				state.Index.PendingRebuilds = filtered
			}
			return nil
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to update state: %v\n", err)
			os.Exit(1)
		}
	}

	if manifestChanged {
		fmt.Printf("rebuild complete: namespace=%s manifest=%s\n", namespaceArg, newManifestKey)
	} else {
		fmt.Printf("rebuild complete: namespace=%s manifest unchanged\n", namespaceArg)
	}
}

func splitFields(raw string) []string {
	parts := strings.Split(raw, ",")
	var fields []string
	for _, part := range parts {
		field := strings.TrimSpace(part)
		if field != "" {
			fields = append(fields, field)
		}
	}
	return fields
}

func hasFTSKey(seg *index.Segment, field string) bool {
	suffix := "/fts/" + field + ".idx"
	for _, key := range seg.FTSKeys {
		if strings.HasSuffix(key, suffix) {
			return true
		}
	}
	return false
}

func buildFTSIndexForSegment(ctx context.Context, store objectstore.Store, docsKey, field string, rawConfig json.RawMessage) (*fts.Index, error) {
	reader, closeFn, err := openDocsReader(ctx, store, docsKey)
	if err != nil {
		return nil, err
	}
	defer closeFn()

	decoder := json.NewDecoder(reader)
	token, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("failed to read JSON start: %w", err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '[' {
		return nil, fmt.Errorf("expected JSON array, got %v", token)
	}

	cfg, err := fts.Parse(rawConfig)
	if err != nil || cfg == nil {
		cfg = fts.DefaultConfig()
	}
	idx := fts.NewIndex(field, cfg)

	var rowID uint32
	for decoder.More() {
		var doc segmentDoc
		if err := decoder.Decode(&doc); err != nil {
			return nil, fmt.Errorf("failed to decode doc: %w", err)
		}
		if !doc.Deleted && doc.Attributes != nil {
			if val, ok := doc.Attributes[field]; ok {
				if text, ok := val.(string); ok && text != "" {
					idx.AddDocument(rowID, text)
				}
			}
		}
		rowID++
	}

	return idx, nil
}

func openDocsReader(ctx context.Context, store objectstore.Store, key string) (io.Reader, func() error, error) {
	reader, _, err := store.Get(ctx, key, nil)
	if err != nil {
		return nil, nil, err
	}

	magic := make([]byte, 4)
	n, err := io.ReadFull(reader, magic)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		reader.Close()
		return nil, nil, fmt.Errorf("failed to read docs magic: %w", err)
	}
	if n == 0 {
		reader.Close()
		return nil, nil, io.EOF
	}

	fullReader := io.MultiReader(bytes.NewReader(magic[:n]), reader)
	isZstd := n >= 4 && magic[0] == 0x28 && magic[1] == 0xB5 && magic[2] == 0x2F && magic[3] == 0xFD
	if !isZstd {
		return fullReader, reader.Close, nil
	}

	zstdReader, err := zstd.NewReader(fullReader,
		zstd.WithDecoderLowmem(true),
		zstd.WithDecoderMaxWindow(32*1024*1024),
		zstd.WithDecoderConcurrency(1),
	)
	if err != nil {
		reader.Close()
		return nil, nil, fmt.Errorf("failed to create zstd reader: %w", err)
	}

	closeFn := func() error {
		zstdReader.Close()
		return reader.Close()
	}
	return zstdReader, closeFn, nil
}

func ensureDocOffsets(ctx context.Context, store objectstore.Store, docsKey string) error {
	offsetsKey := docsOffsetsKey(docsKey)
	if offsetsKey == "" {
		return nil
	}
	if _, err := store.Head(ctx, offsetsKey); err == nil {
		return nil
	}

	reader, closeFn, err := openDocsReader(ctx, store, docsKey)
	if err != nil {
		return err
	}
	defer closeFn()

	decoder := json.NewDecoder(reader)
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("failed to read JSON start: %w", err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("expected JSON array, got %v", token)
	}

	var offsets []uint64
	var lastEnd int64
	for decoder.More() {
		start := decoder.InputOffset()
		var skip json.RawMessage
		if err := decoder.Decode(&skip); err != nil {
			return fmt.Errorf("failed to decode doc: %w", err)
		}
		lastEnd = decoder.InputOffset()
		offsets = append(offsets, uint64(start))
	}
	offsets = append(offsets, uint64(lastEnd))

	data, err := encodeDocOffsets(offsets)
	if err != nil {
		return err
	}

	_, err = store.PutIfAbsent(ctx, offsetsKey, bytes.NewReader(data), int64(len(data)), &objectstore.PutOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil && !objectstore.IsConflictError(err) {
		return err
	}
	return nil
}

func docsOffsetsKey(docsKey string) string {
	if strings.HasSuffix(docsKey, "/docs.col.zst") {
		return strings.TrimSuffix(docsKey, "/docs.col.zst") + "/docs.offsets.bin"
	}
	if strings.HasSuffix(docsKey, "docs.col.zst") {
		return strings.TrimSuffix(docsKey, "docs.col.zst") + "docs.offsets.bin"
	}
	return ""
}

func encodeDocOffsets(offsets []uint64) ([]byte, error) {
	if len(offsets) == 0 {
		return nil, fmt.Errorf("empty offsets")
	}
	count := uint64(len(offsets) - 1)
	buf := &bytes.Buffer{}
	buf.WriteString("DOFF")
	if err := binaryWrite(buf, uint32(1)); err != nil {
		return nil, err
	}
	if err := binaryWrite(buf, count); err != nil {
		return nil, err
	}
	for _, off := range offsets {
		if err := binaryWrite(buf, off); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func binaryWrite(buf *bytes.Buffer, value any) error {
	switch v := value.(type) {
	case uint32:
		var b [4]byte
		b[0] = byte(v)
		b[1] = byte(v >> 8)
		b[2] = byte(v >> 16)
		b[3] = byte(v >> 24)
		_, err := buf.Write(b[:])
		return err
	case uint64:
		var b [8]byte
		b[0] = byte(v)
		b[1] = byte(v >> 8)
		b[2] = byte(v >> 16)
		b[3] = byte(v >> 24)
		b[4] = byte(v >> 32)
		b[5] = byte(v >> 40)
		b[6] = byte(v >> 48)
		b[7] = byte(v >> 56)
		_, err := buf.Write(b[:])
		return err
	default:
		return fmt.Errorf("unsupported type")
	}
}
