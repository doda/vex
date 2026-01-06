package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/vexsearch/vex/internal/config"
	"github.com/vexsearch/vex/internal/fts"
	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/pkg/objectstore"
)

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
		docs, err := loadSegmentDocs(ctx, store, seg.DocsKey)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to load docs for %s: %v\n", seg.DocsKey, err)
			os.Exit(1)
		}
		if len(docs) == 0 {
			continue
		}
		for _, field := range fields {
			if hasFTSKey(seg, field) {
				continue
			}
			fmt.Printf("building FTS index: namespace=%s segment=%s field=%s\n", namespaceArg, seg.ID, field)
			idx, err := buildFTSIndexForDocs(docs, field, ftsConfigRaw)
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
	suffix := "/fts." + field + ".bm25"
	for _, key := range seg.FTSKeys {
		if strings.HasSuffix(key, suffix) {
			return true
		}
	}
	return false
}

func buildFTSIndexForDocs(docs []index.DocColumn, field string, rawConfig json.RawMessage) (*fts.Index, error) {
	cfg, err := fts.Parse(rawConfig)
	if err != nil || cfg == nil {
		cfg = fts.DefaultConfig()
	}
	idx := fts.NewIndex(field, cfg)
	for rowID, doc := range docs {
		if doc.Deleted || doc.Attributes == nil {
			continue
		}
		if val, ok := doc.Attributes[field]; ok {
			if text, ok := val.(string); ok && text != "" {
				idx.AddDocument(uint32(rowID), text)
			}
		}
	}
	return idx, nil
}

func loadSegmentDocs(ctx context.Context, store objectstore.Store, docsKey string) ([]index.DocColumn, error) {
	reader, _, err := store.Get(ctx, docsKey, nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}

	decoded := data
	if index.IsZstdCompressed(data) {
		decoded, err = index.DecompressZstd(data)
		if err != nil {
			return nil, err
		}
	}

	docs, err := index.DecodeDocsColumn(decoded)
	if err == nil {
		return docs, nil
	}
	if errors.Is(err, index.ErrDocsColumnFormat) || errors.Is(err, index.ErrDocsColumnVersion) {
		var fallbackDocs []index.DocColumn
		if err := json.Unmarshal(decoded, &fallbackDocs); err != nil {
			return nil, fmt.Errorf("failed to parse docs JSON: %w", err)
		}
		return fallbackDocs, nil
	}
	return nil, err
}
