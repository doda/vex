package query

import (
	"errors"

	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/namespace"
)

// Rebuild kind constants matching those in write package.
const (
	RebuildKindFilter = "filter"
	RebuildKindFTS    = "fts"
)

// ErrIndexRebuilding indicates a query depends on an index that is still being rebuilt.
var ErrIndexRebuilding = errors.New("query depends on index still being rebuilt")

// CheckPendingRebuilds checks if the query depends on any pending (not ready) index rebuilds.
// Returns ErrIndexRebuilding if the query should return HTTP 202.
//
// A query depends on a pending rebuild if:
// - It uses a filter on an attribute with a pending filter rebuild
// - It uses BM25 ranking on a field with a pending FTS rebuild
// - It uses a composite rank_by that includes BM25 on a field with pending FTS rebuild
func CheckPendingRebuilds(state *namespace.State, f *filter.Filter, parsed *ParsedRankBy) error {
	if state == nil || len(state.Index.PendingRebuilds) == 0 {
		return nil
	}

	// Check if any filter attributes have pending filter rebuilds
	if f != nil {
		filterAttrs := f.UsedAttributes()
		for _, attr := range filterAttrs {
			if hasPendingRebuild(state, RebuildKindFilter, attr) {
				return ErrIndexRebuilding
			}
		}
	}

	// Check if rank_by uses BM25 on a field with pending FTS rebuild
	if parsed != nil {
		bm25Fields := collectBM25Fields(parsed)
		for _, field := range bm25Fields {
			if hasPendingRebuild(state, RebuildKindFTS, field) {
				return ErrIndexRebuilding
			}
		}
	}

	return nil
}

// hasPendingRebuild checks if there's a pending (not ready) rebuild for the given kind/attribute.
func hasPendingRebuild(state *namespace.State, kind, attribute string) bool {
	for _, pr := range state.Index.PendingRebuilds {
		if pr.Kind == kind && pr.Attribute == attribute && !pr.Ready {
			return true
		}
	}
	return false
}

// collectBM25Fields returns all BM25 field names used in a rank_by expression.
func collectBM25Fields(parsed *ParsedRankBy) []string {
	if parsed == nil {
		return nil
	}

	var fields []string

	switch parsed.Type {
	case RankByBM25:
		if parsed.Field != "" {
			fields = append(fields, parsed.Field)
		}
	case RankByComposite:
		// For composite rank_by, recursively collect from the clause
		if parsed.Clause != nil {
			fields = append(fields, collectBM25FieldsFromClause(parsed.Clause)...)
		}
	}

	return fields
}

// collectBM25FieldsFromClause recursively collects BM25 field names from a rank clause.
func collectBM25FieldsFromClause(clause *RankClause) []string {
	if clause == nil {
		return nil
	}

	var fields []string

	switch clause.Type {
	case RankClauseBM25:
		if clause.Field != "" {
			fields = append(fields, clause.Field)
		}
	case RankClauseSum, RankClauseMax:
		for _, sub := range clause.Clauses {
			fields = append(fields, collectBM25FieldsFromClause(sub)...)
		}
	case RankClauseProduct:
		if clause.Clause != nil {
			fields = append(fields, collectBM25FieldsFromClause(clause.Clause)...)
		}
	}

	return fields
}
