package integration

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/metrics"
	"github.com/vexsearch/vex/internal/namespace"
)

func TestBackgroundCompactorCompactsL0Segments(t *testing.T) {
	ns := fmt.Sprintf("objectstore-compaction-%d", time.Now().UnixNano())
	fixture := newS3Fixture(t, ns)
	defer fixture.close(t)

	fixture.write(t, ns, map[string]any{
		"upsert_rows": []map[string]any{
			{"id": "doc1", "text": "hello one"},
		},
	})
	waitForStateHeadSeq(t, fixture.store, ns, 1)
	indexNamespaceRange(t, fixture.store, ns, 0, 1)

	fixture.write(t, ns, map[string]any{
		"upsert_rows": []map[string]any{
			{"id": "doc2", "text": "hello two"},
		},
	})
	waitForStateHeadSeq(t, fixture.store, ns, 2)
	indexNamespaceRange(t, fixture.store, ns, 1, 2)

	fixture.write(t, ns, map[string]any{
		"upsert_rows": []map[string]any{
			{"id": "doc3", "text": "hello three"},
		},
	})
	waitForStateHeadSeq(t, fixture.store, ns, 3)
	indexNamespaceRange(t, fixture.store, ns, 2, 3)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	state, err := readState(ctx, fixture.store, ns)
	cancel()
	if err != nil {
		t.Fatalf("failed to read state: %v", err)
	}
	manifestKey := state.Index.ManifestKey
	if manifestKey == "" && state.Index.ManifestSeq != 0 {
		manifestKey = index.ManifestKey(ns, state.Index.ManifestSeq)
	}
	if manifestKey == "" {
		t.Fatalf("expected manifest key after indexing")
	}

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	manifest, err := readManifest(ctx, fixture.store, manifestKey)
	cancel()
	if err != nil {
		t.Fatalf("failed to read manifest: %v", err)
	}
	if manifest == nil || len(manifest.Segments) != 3 {
		count := 0
		if manifest != nil {
			count = len(manifest.Segments)
		}
		t.Fatalf("expected 3 segments before compaction, got %d", count)
	}

	stateMan := namespace.NewStateManager(fixture.store)
	lsmConfig := &index.LSMConfig{
		L0CompactionThreshold: 2,
		L1CompactionThreshold: 4,
		L0TargetSizeBytes:     index.DefaultL0TargetSizeBytes,
		L1TargetSizeBytes:     index.DefaultL1TargetSizeBytes,
		L2TargetSizeBytes:     index.DefaultL2TargetSizeBytes,
	}
	compactor := index.NewBackgroundCompactor(fixture.store, lsmConfig, &index.CompactorConfig{RetentionTime: 0})
	compactor.SetStateManager(stateMan)
	compactor.RegisterNamespace(ns, index.LoadLSMTree(ns, fixture.store, manifest, lsmConfig))

	if err := compactor.TriggerCompaction(ns); err != nil {
		t.Fatalf("trigger compaction failed: %v", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	state, err = readState(ctx, fixture.store, ns)
	cancel()
	if err != nil {
		t.Fatalf("failed to read state after compaction: %v", err)
	}
	manifestKey = state.Index.ManifestKey
	if manifestKey == "" && state.Index.ManifestSeq != 0 {
		manifestKey = index.ManifestKey(ns, state.Index.ManifestSeq)
	}
	if manifestKey == "" {
		t.Fatalf("expected manifest key after compaction")
	}

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	manifest, err = readManifest(ctx, fixture.store, manifestKey)
	cancel()
	if err != nil {
		t.Fatalf("failed to read manifest after compaction: %v", err)
	}
	if manifest == nil {
		t.Fatalf("expected manifest after compaction")
	}

	l0Count := 0
	l1Count := 0
	for _, seg := range manifest.Segments {
		switch seg.Level {
		case index.L0:
			l0Count++
		case index.L1:
			l1Count++
		}
	}
	if l0Count != 0 {
		t.Fatalf("expected 0 L0 segments after compaction, got %d", l0Count)
	}
	if l1Count != 1 {
		t.Fatalf("expected 1 L1 segment after compaction, got %d", l1Count)
	}
}

func TestSegmentCountMetrics(t *testing.T) {
	metrics.SegmentCounts.Reset()
	metrics.DocumentsIndexedCurrent.Reset()

	ns := fmt.Sprintf("objectstore-segment-metrics-%d", time.Now().UnixNano())
	fixture := newS3Fixture(t, ns)
	defer fixture.close(t)

	fixture.write(t, ns, map[string]any{
		"upsert_rows": []map[string]any{
			{"id": "doc1", "text": "alpha"},
		},
	})
	waitForStateHeadSeq(t, fixture.store, ns, 1)
	indexNamespaceRange(t, fixture.store, ns, 0, 1)

	fixture.write(t, ns, map[string]any{
		"upsert_rows": []map[string]any{
			{"id": "doc2", "text": "beta"},
		},
	})
	waitForStateHeadSeq(t, fixture.store, ns, 2)
	indexNamespaceRange(t, fixture.store, ns, 1, 2)

	fixture.query(t, ns, map[string]any{
		"rank_by": []any{"id", "asc"},
		"limit":   5,
	})

	resp, err := http.Get(fixture.endpoint + "/metrics")
	if err != nil {
		t.Fatalf("metrics request failed: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read metrics response: %v", err)
	}
	metricsText := string(body)

	assertMetricValue(t, metricsText, "vex_segment_count", map[string]string{"namespace": ns, "level": "total"}, 2)
	assertMetricValue(t, metricsText, "vex_segment_count", map[string]string{"namespace": ns, "level": "l0"}, 2)
	assertMetricValue(t, metricsText, "vex_segment_count", map[string]string{"namespace": ns, "level": "l1"}, 0)
	assertMetricValue(t, metricsText, "vex_segment_count", map[string]string{"namespace": ns, "level": "l2"}, 0)
	assertMetricValue(t, metricsText, "vex_documents_indexed", map[string]string{"namespace": ns}, 2)
}

func assertMetricValue(t *testing.T, metricsText, name string, labels map[string]string, expected int) {
	t.Helper()
	value, ok := findMetricValue(metricsText, name, labels)
	if !ok {
		t.Fatalf("metric %s with labels %v not found", name, labels)
	}
	if int(value) != expected {
		t.Fatalf("metric %s with labels %v = %v, want %d", name, labels, value, expected)
	}
}

func findMetricValue(metricsText, name string, labels map[string]string) (float64, bool) {
	for _, line := range strings.Split(metricsText, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		prefix := name + "{"
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		end := strings.Index(line, "}")
		if end == -1 {
			continue
		}
		labelPart := line[len(prefix):end]
		labelMap := parseMetricLabels(labelPart)
		if !labelsMatch(labelMap, labels) {
			continue
		}
		valueStr := strings.TrimSpace(line[end+1:])
		fields := strings.Fields(valueStr)
		if len(fields) == 0 {
			continue
		}
		value, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			return 0, false
		}
		return value, true
	}
	return 0, false
}

func parseMetricLabels(labelPart string) map[string]string {
	labels := map[string]string{}
	if strings.TrimSpace(labelPart) == "" {
		return labels
	}
	for _, part := range strings.Split(labelPart, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		key := kv[0]
		value := strings.Trim(kv[1], "\"")
		labels[key] = value
	}
	return labels
}

func labelsMatch(got map[string]string, want map[string]string) bool {
	for key, value := range want {
		if got[key] != value {
			return false
		}
	}
	return true
}
