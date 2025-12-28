package query

import (
	"testing"

	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/namespace"
)

func TestCheckPendingRebuilds_FilterAttributes(t *testing.T) {
	tests := []struct {
		name           string
		state          *namespace.State
		filterExpr     any
		wantErr        bool
	}{
		{
			name:  "no pending rebuilds",
			state: &namespace.State{Index: namespace.IndexState{}},
			filterExpr: []any{"status", "Eq", "active"},
			wantErr: false,
		},
		{
			name: "pending rebuild on different attribute",
			state: &namespace.State{Index: namespace.IndexState{
				PendingRebuilds: []namespace.PendingRebuild{
					{Kind: RebuildKindFilter, Attribute: "other_attr", Ready: false},
				},
			}},
			filterExpr: []any{"status", "Eq", "active"},
			wantErr: false,
		},
		{
			name: "pending rebuild on filtered attribute",
			state: &namespace.State{Index: namespace.IndexState{
				PendingRebuilds: []namespace.PendingRebuild{
					{Kind: RebuildKindFilter, Attribute: "status", Ready: false},
				},
			}},
			filterExpr: []any{"status", "Eq", "active"},
			wantErr: true,
		},
		{
			name: "ready rebuild on filtered attribute - no error",
			state: &namespace.State{Index: namespace.IndexState{
				PendingRebuilds: []namespace.PendingRebuild{
					{Kind: RebuildKindFilter, Attribute: "status", Ready: true},
				},
			}},
			filterExpr: []any{"status", "Eq", "active"},
			wantErr: false,
		},
		{
			name: "pending rebuild in nested filter",
			state: &namespace.State{Index: namespace.IndexState{
				PendingRebuilds: []namespace.PendingRebuild{
					{Kind: RebuildKindFilter, Attribute: "category", Ready: false},
				},
			}},
			filterExpr: []any{"And", []any{
				[]any{"status", "Eq", "active"},
				[]any{"category", "Eq", "tech"},
			}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := filter.Parse(tt.filterExpr)
			if err != nil {
				t.Fatalf("failed to parse filter: %v", err)
			}

			err = CheckPendingRebuilds(tt.state, f, nil)
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestCheckPendingRebuilds_BM25Fields(t *testing.T) {
	tests := []struct {
		name     string
		state    *namespace.State
		parsed   *ParsedRankBy
		wantErr  bool
	}{
		{
			name: "no pending rebuilds",
			state: &namespace.State{Index: namespace.IndexState{}},
			parsed: &ParsedRankBy{Type: RankByBM25, Field: "content"},
			wantErr: false,
		},
		{
			name: "pending FTS rebuild on different field",
			state: &namespace.State{Index: namespace.IndexState{
				PendingRebuilds: []namespace.PendingRebuild{
					{Kind: RebuildKindFTS, Attribute: "other_field", Ready: false},
				},
			}},
			parsed: &ParsedRankBy{Type: RankByBM25, Field: "content"},
			wantErr: false,
		},
		{
			name: "pending FTS rebuild on BM25 field",
			state: &namespace.State{Index: namespace.IndexState{
				PendingRebuilds: []namespace.PendingRebuild{
					{Kind: RebuildKindFTS, Attribute: "content", Ready: false},
				},
			}},
			parsed: &ParsedRankBy{Type: RankByBM25, Field: "content"},
			wantErr: true,
		},
		{
			name: "ready FTS rebuild - no error",
			state: &namespace.State{Index: namespace.IndexState{
				PendingRebuilds: []namespace.PendingRebuild{
					{Kind: RebuildKindFTS, Attribute: "content", Ready: true, Version: 1},
				},
			}},
			parsed: &ParsedRankBy{Type: RankByBM25, Field: "content"},
			wantErr: false,
		},
		{
			name: "pending rebuild in composite rank_by",
			state: &namespace.State{Index: namespace.IndexState{
				PendingRebuilds: []namespace.PendingRebuild{
					{Kind: RebuildKindFTS, Attribute: "title", Ready: false},
				},
			}},
			parsed: &ParsedRankBy{
				Type: RankByComposite,
				Clause: &RankClause{
					Type: RankClauseSum,
					Clauses: []*RankClause{
						{Type: RankClauseBM25, Field: "title"},
						{Type: RankClauseBM25, Field: "content"},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "vector rank_by - no FTS check needed",
			state: &namespace.State{Index: namespace.IndexState{
				PendingRebuilds: []namespace.PendingRebuild{
					{Kind: RebuildKindFTS, Attribute: "content", Ready: false},
				},
			}},
			parsed: &ParsedRankBy{Type: RankByVector, QueryVector: []float32{0.1, 0.2, 0.3}},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckPendingRebuilds(tt.state, nil, tt.parsed)
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestCheckPendingRebuilds_NilInputs(t *testing.T) {
	// All nil inputs should not return error
	err := CheckPendingRebuilds(nil, nil, nil)
	if err != nil {
		t.Errorf("expected nil error for nil state, got %v", err)
	}

	// State with empty pending rebuilds
	err = CheckPendingRebuilds(&namespace.State{}, nil, nil)
	if err != nil {
		t.Errorf("expected nil error for empty state, got %v", err)
	}
}
