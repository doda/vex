package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/vexsearch/vex/internal/config"
	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/pkg/objectstore"
)

var docsColumnMagic = [4]byte{0x56, 0x45, 0x58, 0x44}

const (
	dropModeCorruptOnly  = "corrupt-only"
	dropModeFromEarliest = "from-earliest"
	maxManifestSeqSkew   = 25
	maxApplyAttempts     = 5
)

type segmentIssue struct {
	Segment index.Segment
	Err     error
	Kind    string
}

func main() {
	var (
		configPath    string
		namespaceArg  string
		dropMode      string
		apply         bool
		deleteObjects bool
	)

	flag.StringVar(&configPath, "config", "", "Path to vex config file")
	flag.StringVar(&namespaceArg, "namespace", "", "Namespace to scan")
	flag.StringVar(&dropMode, "drop-mode", dropModeCorruptOnly, "Drop mode: corrupt-only or from-earliest")
	flag.BoolVar(&apply, "apply", false, "Write new manifest to drop corrupted segments")
	flag.BoolVar(&deleteObjects, "delete-objects", false, "Delete objects for removed segments (requires -apply)")
	flag.Parse()

	if configPath == "" || namespaceArg == "" {
		fmt.Fprintln(os.Stderr, "Usage: repair_docs -config <path> -namespace <ns> [-drop-mode corrupt-only|from-earliest] [-apply] [-delete-objects]")
		os.Exit(2)
	}
	if dropMode != dropModeCorruptOnly && dropMode != dropModeFromEarliest {
		fmt.Fprintln(os.Stderr, "drop-mode must be corrupt-only or from-earliest")
		os.Exit(2)
	}
	if deleteObjects && !apply {
		fmt.Fprintln(os.Stderr, "-delete-objects requires -apply")
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
	reader := index.NewReader(store, nil, nil)

	loaded, err := stateMan.Load(ctx, namespaceArg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load state: %v\n", err)
		os.Exit(1)
	}

	if loaded.State.Index.ManifestKey == "" {
		fmt.Fprintln(os.Stderr, "namespace has no manifest; aborting")
		os.Exit(1)
	}

	manifest, err := reader.LoadManifest(ctx, loaded.State.Index.ManifestKey)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load manifest: %v\n", err)
		os.Exit(1)
	}
	if manifest == nil {
		fmt.Fprintln(os.Stderr, "manifest not found; aborting")
		os.Exit(1)
	}

	fmt.Printf("loaded manifest: %s segments=%d\n", loaded.State.Index.ManifestKey, len(manifest.Segments))

	var (
		scanned  int
		withDocs int
		issues   []segmentIssue
	)

	for _, seg := range manifest.Segments {
		if seg.DocsKey == "" {
			continue
		}
		withDocs++
		err := scanDocsColumn(ctx, store, seg.DocsKey)
		scanned++
		if err == nil {
			continue
		}
		if errors.Is(err, index.ErrDocsColumnFormat) || errors.Is(err, index.ErrDocsColumnVersion) {
			fmt.Printf("segment %s wal=%d..%d docs not column format (skipping): %v\n", seg.ID, seg.StartWALSeq, seg.EndWALSeq, err)
			continue
		}
		kind := "error"
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			kind = "eof"
		}
		fmt.Printf("segment %s wal=%d..%d docs error (%s): %v\n", seg.ID, seg.StartWALSeq, seg.EndWALSeq, kind, err)
		issues = append(issues, segmentIssue{Segment: seg, Err: err, Kind: kind})
	}

	fmt.Printf("scan complete: segments=%d docs=%d scanned=%d issues=%d\n", len(manifest.Segments), withDocs, scanned, len(issues))

	if len(issues) == 0 {
		return
	}

	if !apply {
		fmt.Println("re-run with -apply to drop corrupted segments")
		return
	}

	if err := applyFix(ctx, store, stateMan, reader, namespaceArg, dropMode, issues, deleteObjects); err != nil {
		fmt.Fprintf(os.Stderr, "apply failed: %v\n", err)
		os.Exit(1)
	}
}

func applyFix(ctx context.Context, store objectstore.Store, stateMan *namespace.StateManager, reader *index.Reader, namespaceArg, dropMode string, issues []segmentIssue, deleteObjects bool) error {
	removeIDs := make(map[string]segmentIssue)
	var earliestStart uint64
	for i, issue := range issues {
		removeIDs[issue.Segment.ID] = issue
		if i == 0 || issue.Segment.StartWALSeq < earliestStart {
			earliestStart = issue.Segment.StartWALSeq
		}
	}

	for attempt := 1; attempt <= maxApplyAttempts; attempt++ {
		loaded, err := stateMan.Load(ctx, namespaceArg)
		if err != nil {
			return fmt.Errorf("failed to reload state: %w", err)
		}
		if loaded.State.Index.ManifestKey == "" {
			return errors.New("namespace has no manifest")
		}

		manifest, err := reader.LoadManifest(ctx, loaded.State.Index.ManifestKey)
		if err != nil {
			return fmt.Errorf("failed to reload manifest: %w", err)
		}
		if manifest == nil {
			return errors.New("manifest not found")
		}

		removalSet := make(map[string]segmentIssue)
		for id, issue := range removeIDs {
			removalSet[id] = issue
		}
		if dropMode == dropModeFromEarliest {
			for _, seg := range manifest.Segments {
				if seg.EndWALSeq >= earliestStart {
					if _, exists := removalSet[seg.ID]; !exists {
						removalSet[seg.ID] = segmentIssue{Segment: seg, Kind: "range"}
					}
				}
			}
		}

		updated := manifest.Clone()
		if updated == nil {
			return errors.New("failed to clone manifest")
		}

		removed := make([]index.Segment, 0, len(removalSet))
		for _, seg := range manifest.Segments {
			if _, ok := removalSet[seg.ID]; !ok {
				continue
			}
			if updated.RemoveSegment(seg.ID) {
				removed = append(removed, seg)
			}
		}

		if len(removed) == 0 {
			return errors.New("no segments removed")
		}

		sort.Slice(removed, func(i, j int) bool {
			return removed[i].StartWALSeq < removed[j].StartWALSeq
		})

		updated.GeneratedAt = time.Now().UTC()
		updated.UpdateIndexedWALSeq()
		manifestData, err := updated.MarshalJSON()
		if err != nil {
			return fmt.Errorf("failed to marshal manifest: %w", err)
		}

		var (
			newManifestSeq uint64
			newManifestKey string
		)
		maxSeq := loaded.State.Index.ManifestSeq + 1 + maxManifestSeqSkew
		for seq := loaded.State.Index.ManifestSeq + 1; seq < maxSeq; seq++ {
			key := index.ManifestKey(namespaceArg, seq)
			_, err = store.PutIfAbsent(ctx, key, bytes.NewReader(manifestData), int64(len(manifestData)), &objectstore.PutOptions{
				ContentType: "application/json",
			})
			if err != nil {
				if objectstore.IsConflictError(err) {
					continue
				}
				return fmt.Errorf("failed to write manifest: %w", err)
			}
			newManifestSeq = seq
			newManifestKey = key
			break
		}
		if newManifestKey == "" {
			continue
		}

		_, err = stateMan.UpdateIndexManifest(ctx, namespaceArg, loaded.ETag, newManifestKey, newManifestSeq, updated.IndexedWALSeq)
		if err != nil {
			return fmt.Errorf("failed to update state: %w", err)
		}

		fmt.Printf("updated manifest: %s removed=%d indexed_wal_seq=%d\n", newManifestKey, len(removed), updated.IndexedWALSeq)

		if deleteObjects {
			for _, seg := range removed {
				for _, key := range segmentKeys(seg) {
					if key == "" {
						continue
					}
					if err := store.Delete(ctx, key); err != nil && !objectstore.IsNotFoundError(err) {
						return fmt.Errorf("failed to delete %s: %w", key, err)
					}
				}
			}
			fmt.Printf("deleted objects for %d segments\n", len(removed))
		}

		return nil
	}

	return fmt.Errorf("failed to apply after %d attempts (manifest keeps changing)", maxApplyAttempts)
}

func scanDocsColumn(ctx context.Context, store objectstore.Store, docsKey string) error {
	reader, _, err := store.Get(ctx, docsKey, nil)
	if err != nil {
		return err
	}
	defer reader.Close()

	magic := make([]byte, 4)
	n, err := io.ReadFull(reader, magic)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return fmt.Errorf("failed to read docs magic: %w", err)
	}
	if n == 0 {
		return nil
	}

	fullReader := io.MultiReader(bytes.NewReader(magic[:n]), reader)
	if n >= 4 && magic[0] == 0x28 && magic[1] == 0xB5 && magic[2] == 0x2F && magic[3] == 0xFD {
		dec, err := zstd.NewReader(fullReader,
			zstd.WithDecoderLowmem(true),
			zstd.WithDecoderMaxWindow(32*1024*1024),
			zstd.WithDecoderConcurrency(1),
		)
		if err != nil {
			return fmt.Errorf("failed to create zstd reader: %w", err)
		}
		defer dec.Close()
		fullReader = dec
	}

	buffered := bufio.NewReader(fullReader)
	peek, err := buffered.Peek(len(docsColumnMagic))
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return fmt.Errorf("failed to peek docs data: %w", err)
	}
	if len(peek) < len(docsColumnMagic) {
		return io.ErrUnexpectedEOF
	}
	if !bytes.Equal(peek, docsColumnMagic[:]) {
		return index.ErrDocsColumnFormat
	}

	idSet := map[uint32]struct{}{0: {}}
	_, err = index.DecodeDocsColumnForRowIDs(buffered, idSet)
	return err
}

func segmentKeys(seg index.Segment) []string {
	keys := make([]string, 0, 8)
	if seg.DocsKey != "" {
		keys = append(keys, seg.DocsKey)
		offsetsKey := index.DocsOffsetsKey(seg.DocsKey)
		if offsetsKey != "" {
			keys = append(keys, offsetsKey)
		}
		idMapKey := index.DocsIDMapKey(seg.DocsKey)
		if idMapKey != "" {
			keys = append(keys, idMapKey)
		}
	}
	if seg.VectorsKey != "" {
		keys = append(keys, seg.VectorsKey)
	}
	if seg.IVFKeys != nil {
		keys = append(keys, seg.IVFKeys.AllKeys()...)
	}
	if len(seg.FilterKeys) > 0 {
		keys = append(keys, seg.FilterKeys...)
	}
	if len(seg.FTSKeys) > 0 {
		keys = append(keys, seg.FTSKeys...)
	}
	return keys
}
