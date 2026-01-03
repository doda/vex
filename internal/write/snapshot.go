package write

import (
	"context"
	"fmt"
	"sort"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/tail"
)

func (h *Handler) loadSnapshotDocMap(ctx context.Context, ns string, snapshotSeq uint64, state *namespace.State, tailErr error) (map[string]*tail.Document, error) {
	indexedSeq := state.Index.IndexedWALSeq
	if snapshotSeq < indexedSeq {
		indexedSeq = snapshotSeq
	}

	if h.tailStore == nil {
		return nil, tailErr
	}

	if snapshotSeq > indexedSeq {
		if err := h.tailStore.Refresh(ctx, ns, indexedSeq, snapshotSeq); err != nil {
			return nil, fmt.Errorf("failed to refresh tail: %w", err)
		}
	}

	var docs []*tail.Document

	if state.Index.ManifestKey != "" && h.indexReader != nil {
		indexedDocs, err := h.indexReader.LoadSegmentDocs(ctx, state.Index.ManifestKey)
		if err != nil {
			return nil, fmt.Errorf("failed to load indexed docs: %w", err)
		}
		for _, idoc := range indexedDocs {
			docID, ok := indexedDocumentIDForWrite(idoc)
			if !ok {
				continue
			}
			docs = append(docs, &tail.Document{
				ID:         docID,
				WalSeq:     idoc.WALSeq,
				SubBatchID: 0,
				Attributes: idoc.Attributes,
				Deleted:    idoc.Deleted,
			})
		}
	}

	if snapshotSeq > indexedSeq && h.tailStore != nil {
		tailDocs, err := h.scanTailIncludingDeleted(ctx, ns)
		if err != nil {
			return nil, err
		}
		for _, doc := range tailDocs {
			if doc.WalSeq <= indexedSeq {
				continue
			}
			docs = append(docs, doc)
		}
	}

	return dedupeSnapshotDocs(docs), nil
}

func (h *Handler) scanTailIncludingDeleted(ctx context.Context, ns string) ([]*tail.Document, error) {
	type scanIncludingDeleted interface {
		ScanIncludingDeleted(ctx context.Context, namespace string, f *filter.Filter) ([]*tail.Document, error)
	}

	if h.tailStore == nil {
		return nil, nil
	}

	if scanner, ok := h.tailStore.(scanIncludingDeleted); ok {
		return scanner.ScanIncludingDeleted(ctx, ns, nil)
	}
	return h.tailStore.Scan(ctx, ns, nil)
}

func dedupeSnapshotDocs(docs []*tail.Document) map[string]*tail.Document {
	docMap := make(map[string]*tail.Document)
	for _, doc := range docs {
		key := doc.ID.String()
		existing, ok := docMap[key]
		if !ok || doc.WalSeq > existing.WalSeq || (doc.WalSeq == existing.WalSeq && doc.SubBatchID > existing.SubBatchID) {
			docMap[key] = doc
		}
	}
	return docMap
}

func snapshotDocsFromMap(docMap map[string]*tail.Document) []*tail.Document {
	docs := make([]*tail.Document, 0, len(docMap))
	for _, doc := range docMap {
		docs = append(docs, doc)
	}

	sort.Slice(docs, func(i, j int) bool {
		if docs[i].WalSeq != docs[j].WalSeq {
			return docs[i].WalSeq > docs[j].WalSeq
		}
		return docs[i].SubBatchID > docs[j].SubBatchID
	})

	return docs
}

func indexedDocumentIDForWrite(idoc index.IndexedDocument) (document.ID, bool) {
	if idoc.ID != "" {
		docID, err := document.ParseID(idoc.ID)
		if err == nil {
			return docID, true
		}
		docID, err = document.NewStringID(idoc.ID)
		if err == nil {
			return docID, true
		}
		return document.ID{}, false
	}
	if idoc.NumericID != 0 {
		return document.NewU64ID(idoc.NumericID), true
	}
	return document.ID{}, false
}

func buildSnapshotFilterDoc(doc *tail.Document) filter.Document {
	filterDoc := make(filter.Document)
	for k, v := range doc.Attributes {
		filterDoc[k] = v
	}
	filterDoc["id"] = docIDToFilterValue(doc.ID)
	return filterDoc
}

func docIDToFilterValue(id document.ID) any {
	switch id.Type() {
	case document.IDTypeU64:
		return id.U64()
	case document.IDTypeUUID:
		return id.UUID().String()
	case document.IDTypeString:
		return id.String()
	default:
		return id.String()
	}
}
