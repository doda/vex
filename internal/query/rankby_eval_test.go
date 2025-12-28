package query

import (
	"math"
	"testing"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/fts"
	"github.com/vexsearch/vex/internal/tail"
)

// TestSumOperatorEvaluation tests the Sum operator score evaluation behavior.
func TestSumOperatorEvaluation(t *testing.T) {
	docs := []*tail.Document{
		{
			ID:         document.NewU64ID(1),
			Attributes: map[string]any{"title": "apple banana", "content": "cherry date"},
		},
		{
			ID:         document.NewU64ID(2),
			Attributes: map[string]any{"title": "apple", "content": "apple apple"},
		},
		{
			ID:         document.NewU64ID(3),
			Attributes: map[string]any{"title": "banana", "content": "cherry"},
		},
	}

	configs := map[string]*fts.Config{
		"title":   fts.DefaultConfig(),
		"content": fts.DefaultConfig(),
	}

	t.Run("Sum combines BM25 scores additively", func(t *testing.T) {
		clause := &RankClause{
			Type: RankClauseSum,
			Clauses: []*RankClause{
				{Type: RankClauseBM25, Field: "title", QueryText: "apple"},
				{Type: RankClauseBM25, Field: "content", QueryText: "apple"},
			},
		}

		scorer := NewRankScorer(clause, configs)
		scorer.BuildIndexes(docs)

		// Doc 1: apple in title (1 match) only
		// Doc 2: apple in title (1 match) + apple in content (2 matches)
		// Doc 3: no apple
		score1 := scorer.Score(docs[0], 0)
		score2 := scorer.Score(docs[1], 1)
		score3 := scorer.Score(docs[2], 2)

		if score1 <= 0 {
			t.Errorf("expected positive score for doc 1, got %f", score1)
		}
		if score2 <= 0 {
			t.Errorf("expected positive score for doc 2, got %f", score2)
		}
		if score3 != 0 {
			t.Errorf("expected score 0 for doc 3, got %f", score3)
		}
		// Doc 2 should have higher score (apple in both fields)
		if score2 <= score1 {
			t.Errorf("expected doc 2 score (%f) > doc 1 score (%f)", score2, score1)
		}
	})

	t.Run("Sum with empty clause list returns zero", func(t *testing.T) {
		clause := &RankClause{
			Type:    RankClauseSum,
			Clauses: []*RankClause{},
		}

		scorer := NewRankScorer(clause, configs)
		scorer.BuildIndexes(docs)

		score := scorer.Score(docs[0], 0)
		if score != 0 {
			t.Errorf("expected 0 for empty Sum, got %f", score)
		}
	})

	t.Run("Sum with single clause equals clause score", func(t *testing.T) {
		innerClause := &RankClause{Type: RankClauseBM25, Field: "title", QueryText: "apple"}
		sumClause := &RankClause{
			Type:    RankClauseSum,
			Clauses: []*RankClause{innerClause},
		}

		scorerInner := NewRankScorer(innerClause, configs)
		scorerInner.BuildIndexes(docs)
		innerScore := scorerInner.Score(docs[0], 0)

		scorerSum := NewRankScorer(sumClause, configs)
		scorerSum.BuildIndexes(docs)
		sumScore := scorerSum.Score(docs[0], 0)

		if math.Abs(innerScore-sumScore) > 0.0001 {
			t.Errorf("expected Sum of single clause (%f) to equal clause score (%f)", sumScore, innerScore)
		}
	})

	t.Run("Sum with multiple clauses is associative", func(t *testing.T) {
		// Sum([a, b, c]) should equal Sum([Sum([a, b]), c])
		clauseA := &RankClause{Type: RankClauseBM25, Field: "title", QueryText: "apple"}
		clauseB := &RankClause{Type: RankClauseBM25, Field: "content", QueryText: "cherry"}
		clauseC := &RankClause{Type: RankClauseBM25, Field: "title", QueryText: "banana"}

		flatSum := &RankClause{
			Type:    RankClauseSum,
			Clauses: []*RankClause{clauseA, clauseB, clauseC},
		}

		nestedSum := &RankClause{
			Type: RankClauseSum,
			Clauses: []*RankClause{
				{Type: RankClauseSum, Clauses: []*RankClause{clauseA, clauseB}},
				clauseC,
			},
		}

		scorerFlat := NewRankScorer(flatSum, configs)
		scorerFlat.BuildIndexes(docs)
		flatScore := scorerFlat.Score(docs[0], 0)

		scorerNested := NewRankScorer(nestedSum, configs)
		scorerNested.BuildIndexes(docs)
		nestedScore := scorerNested.Score(docs[0], 0)

		if math.Abs(flatScore-nestedScore) > 0.0001 {
			t.Errorf("Sum should be associative: flat=%f, nested=%f", flatScore, nestedScore)
		}
	})
}

// TestMaxOperatorEvaluation tests the Max operator score evaluation behavior.
func TestMaxOperatorEvaluation(t *testing.T) {
	docs := []*tail.Document{
		{
			ID:         document.NewU64ID(1),
			Attributes: map[string]any{"title": "apple apple apple", "content": "banana"},
		},
		{
			ID:         document.NewU64ID(2),
			Attributes: map[string]any{"title": "cherry", "content": "apple apple apple apple"},
		},
	}

	configs := map[string]*fts.Config{
		"title":   fts.DefaultConfig(),
		"content": fts.DefaultConfig(),
	}

	t.Run("Max selects highest score among clauses", func(t *testing.T) {
		// Score for apple in title vs apple in content
		titleClause := &RankClause{Type: RankClauseBM25, Field: "title", QueryText: "apple"}
		contentClause := &RankClause{Type: RankClauseBM25, Field: "content", QueryText: "apple"}
		maxClause := &RankClause{
			Type:    RankClauseMax,
			Clauses: []*RankClause{titleClause, contentClause},
		}

		scorerTitle := NewRankScorer(titleClause, configs)
		scorerTitle.BuildIndexes(docs)
		titleScore1 := scorerTitle.Score(docs[0], 0)

		scorerContent := NewRankScorer(contentClause, configs)
		scorerContent.BuildIndexes(docs)
		contentScore1 := scorerContent.Score(docs[0], 0)

		scorerMax := NewRankScorer(maxClause, configs)
		scorerMax.BuildIndexes(docs)
		maxScore1 := scorerMax.Score(docs[0], 0)

		// Max should be the higher of the two
		expectedMax := math.Max(titleScore1, contentScore1)
		if math.Abs(maxScore1-expectedMax) > 0.0001 {
			t.Errorf("Max score (%f) should equal max of components (%f, %f)", maxScore1, titleScore1, contentScore1)
		}
	})

	t.Run("Max with empty clause list returns zero", func(t *testing.T) {
		clause := &RankClause{
			Type:    RankClauseMax,
			Clauses: []*RankClause{},
		}

		scorer := NewRankScorer(clause, configs)
		scorer.BuildIndexes(docs)

		score := scorer.Score(docs[0], 0)
		if score != 0 {
			t.Errorf("expected 0 for empty Max, got %f", score)
		}
	})

	t.Run("Max with single clause equals clause score", func(t *testing.T) {
		innerClause := &RankClause{Type: RankClauseBM25, Field: "title", QueryText: "apple"}
		maxClause := &RankClause{
			Type:    RankClauseMax,
			Clauses: []*RankClause{innerClause},
		}

		scorerInner := NewRankScorer(innerClause, configs)
		scorerInner.BuildIndexes(docs)
		innerScore := scorerInner.Score(docs[0], 0)

		scorerMax := NewRankScorer(maxClause, configs)
		scorerMax.BuildIndexes(docs)
		maxScore := scorerMax.Score(docs[0], 0)

		if math.Abs(innerScore-maxScore) > 0.0001 {
			t.Errorf("expected Max of single clause (%f) to equal clause score (%f)", maxScore, innerScore)
		}
	})

	t.Run("Max picks correct clause for different documents", func(t *testing.T) {
		// Doc 1: apple in title (high), banana in content
		// Doc 2: cherry in title, apple in content (high)
		titleClause := &RankClause{Type: RankClauseBM25, Field: "title", QueryText: "apple"}
		contentClause := &RankClause{Type: RankClauseBM25, Field: "content", QueryText: "apple"}
		maxClause := &RankClause{
			Type:    RankClauseMax,
			Clauses: []*RankClause{titleClause, contentClause},
		}

		scorerMax := NewRankScorer(maxClause, configs)
		scorerMax.BuildIndexes(docs)

		// Doc 1: apple in title only, doc 2: apple in content only
		score1 := scorerMax.Score(docs[0], 0) // Should use title score
		score2 := scorerMax.Score(docs[1], 1) // Should use content score

		if score1 <= 0 {
			t.Errorf("expected positive score for doc 1, got %f", score1)
		}
		if score2 <= 0 {
			t.Errorf("expected positive score for doc 2, got %f", score2)
		}
	})

	t.Run("Max ignores zero scores from non-matching clauses", func(t *testing.T) {
		// Only one clause will match
		matchingClause := &RankClause{Type: RankClauseBM25, Field: "title", QueryText: "apple"}
		nonMatchingClause := &RankClause{Type: RankClauseBM25, Field: "title", QueryText: "xyz-nonexistent"}
		maxClause := &RankClause{
			Type:    RankClauseMax,
			Clauses: []*RankClause{matchingClause, nonMatchingClause},
		}

		scorerMatching := NewRankScorer(matchingClause, configs)
		scorerMatching.BuildIndexes(docs)
		expectedScore := scorerMatching.Score(docs[0], 0)

		scorerMax := NewRankScorer(maxClause, configs)
		scorerMax.BuildIndexes(docs)
		maxScore := scorerMax.Score(docs[0], 0)

		if math.Abs(maxScore-expectedScore) > 0.0001 {
			t.Errorf("Max should pick matching clause: got %f, expected %f", maxScore, expectedScore)
		}
	})
}

// TestProductOperatorEvaluation tests the Product operator score evaluation behavior.
func TestProductOperatorEvaluation(t *testing.T) {
	docs := []*tail.Document{
		{
			ID:         document.NewU64ID(1),
			Attributes: map[string]any{"title": "apple banana cherry"},
		},
		{
			ID:         document.NewU64ID(2),
			Attributes: map[string]any{"title": "date elderberry"},
		},
	}

	configs := map[string]*fts.Config{
		"title": fts.DefaultConfig(),
	}

	t.Run("Product multiplies weight by clause score", func(t *testing.T) {
		weight := 3.5
		innerClause := &RankClause{Type: RankClauseBM25, Field: "title", QueryText: "apple"}
		productClause := &RankClause{
			Type:   RankClauseProduct,
			Weight: weight,
			Clause: innerClause,
		}

		scorerInner := NewRankScorer(innerClause, configs)
		scorerInner.BuildIndexes(docs)
		innerScore := scorerInner.Score(docs[0], 0)

		scorerProduct := NewRankScorer(productClause, configs)
		scorerProduct.BuildIndexes(docs)
		productScore := scorerProduct.Score(docs[0], 0)

		expectedScore := weight * innerScore
		if math.Abs(productScore-expectedScore) > 0.0001 {
			t.Errorf("Product score (%f) should be weight * inner score (%f * %f = %f)",
				productScore, weight, innerScore, expectedScore)
		}
	})

	t.Run("Product with weight 1.0 equals clause score", func(t *testing.T) {
		innerClause := &RankClause{Type: RankClauseBM25, Field: "title", QueryText: "apple"}
		productClause := &RankClause{
			Type:   RankClauseProduct,
			Weight: 1.0,
			Clause: innerClause,
		}

		scorerInner := NewRankScorer(innerClause, configs)
		scorerInner.BuildIndexes(docs)
		innerScore := scorerInner.Score(docs[0], 0)

		scorerProduct := NewRankScorer(productClause, configs)
		scorerProduct.BuildIndexes(docs)
		productScore := scorerProduct.Score(docs[0], 0)

		if math.Abs(productScore-innerScore) > 0.0001 {
			t.Errorf("Product with weight 1.0 (%f) should equal clause score (%f)", productScore, innerScore)
		}
	})

	t.Run("Product with weight 0.0 returns zero", func(t *testing.T) {
		innerClause := &RankClause{Type: RankClauseBM25, Field: "title", QueryText: "apple"}
		productClause := &RankClause{
			Type:   RankClauseProduct,
			Weight: 0.0,
			Clause: innerClause,
		}

		scorer := NewRankScorer(productClause, configs)
		scorer.BuildIndexes(docs)
		score := scorer.Score(docs[0], 0)

		if score != 0 {
			t.Errorf("Product with weight 0.0 should return 0, got %f", score)
		}
	})

	t.Run("Product with negative weight produces negative score", func(t *testing.T) {
		innerClause := &RankClause{Type: RankClauseBM25, Field: "title", QueryText: "apple"}
		productClause := &RankClause{
			Type:   RankClauseProduct,
			Weight: -2.0,
			Clause: innerClause,
		}

		scorerInner := NewRankScorer(innerClause, configs)
		scorerInner.BuildIndexes(docs)
		innerScore := scorerInner.Score(docs[0], 0)

		scorerProduct := NewRankScorer(productClause, configs)
		scorerProduct.BuildIndexes(docs)
		productScore := scorerProduct.Score(docs[0], 0)

		expectedScore := -2.0 * innerScore
		if math.Abs(productScore-expectedScore) > 0.0001 {
			t.Errorf("Product with negative weight should produce negative score: got %f, expected %f",
				productScore, expectedScore)
		}
	})

	t.Run("Product with fractional weight", func(t *testing.T) {
		innerClause := &RankClause{Type: RankClauseBM25, Field: "title", QueryText: "apple"}
		productClause := &RankClause{
			Type:   RankClauseProduct,
			Weight: 0.5,
			Clause: innerClause,
		}

		scorerInner := NewRankScorer(innerClause, configs)
		scorerInner.BuildIndexes(docs)
		innerScore := scorerInner.Score(docs[0], 0)

		scorerProduct := NewRankScorer(productClause, configs)
		scorerProduct.BuildIndexes(docs)
		productScore := scorerProduct.Score(docs[0], 0)

		expectedScore := 0.5 * innerScore
		if math.Abs(productScore-expectedScore) > 0.0001 {
			t.Errorf("Product with 0.5 weight (%f) should halve inner score (%f)",
				productScore, innerScore)
		}
	})

	t.Run("Nested Product is multiplicative", func(t *testing.T) {
		innerClause := &RankClause{Type: RankClauseBM25, Field: "title", QueryText: "apple"}
		productClause := &RankClause{
			Type:   RankClauseProduct,
			Weight: 2.0,
			Clause: &RankClause{
				Type:   RankClauseProduct,
				Weight: 3.0,
				Clause: innerClause,
			},
		}

		scorerInner := NewRankScorer(innerClause, configs)
		scorerInner.BuildIndexes(docs)
		innerScore := scorerInner.Score(docs[0], 0)

		scorerProduct := NewRankScorer(productClause, configs)
		scorerProduct.BuildIndexes(docs)
		productScore := scorerProduct.Score(docs[0], 0)

		expectedScore := 2.0 * 3.0 * innerScore
		if math.Abs(productScore-expectedScore) > 0.0001 {
			t.Errorf("Nested Product should multiply weights: got %f, expected %f", productScore, expectedScore)
		}
	})
}

// TestFilterBoostEvaluation tests that filter clauses yield exactly 1 or 0.
func TestFilterBoostEvaluation(t *testing.T) {
	docs := []*tail.Document{
		{
			ID:         document.NewU64ID(1),
			Attributes: map[string]any{"category": "premium", "status": "active", "count": int64(10)},
		},
		{
			ID:         document.NewU64ID(2),
			Attributes: map[string]any{"category": "basic", "status": "inactive", "count": int64(5)},
		},
		{
			ID:         document.NewU64ID(3),
			Attributes: map[string]any{"category": "premium", "status": "inactive", "count": int64(20)},
		},
	}

	configs := map[string]*fts.Config{}

	t.Run("Filter Eq match yields score 1", func(t *testing.T) {
		f, _ := filter.Parse([]any{"category", "Eq", "premium"})
		clause := &RankClause{Type: RankClauseFilter, Filter: f}

		scorer := NewRankScorer(clause, configs)
		scorer.BuildIndexes(docs)

		score1 := scorer.Score(docs[0], 0) // premium
		score2 := scorer.Score(docs[1], 1) // basic
		score3 := scorer.Score(docs[2], 2) // premium

		if score1 != 1.0 {
			t.Errorf("expected score 1 for matching filter (doc 1), got %f", score1)
		}
		if score2 != 0.0 {
			t.Errorf("expected score 0 for non-matching filter (doc 2), got %f", score2)
		}
		if score3 != 1.0 {
			t.Errorf("expected score 1 for matching filter (doc 3), got %f", score3)
		}
	})

	t.Run("Filter And combination yields 1 or 0", func(t *testing.T) {
		f, _ := filter.Parse([]any{"And", []any{
			[]any{"category", "Eq", "premium"},
			[]any{"status", "Eq", "active"},
		}})
		clause := &RankClause{Type: RankClauseFilter, Filter: f}

		scorer := NewRankScorer(clause, configs)
		scorer.BuildIndexes(docs)

		// Doc 1: premium + active = match
		// Doc 2: basic + inactive = no match
		// Doc 3: premium + inactive = no match
		score1 := scorer.Score(docs[0], 0)
		score2 := scorer.Score(docs[1], 1)
		score3 := scorer.Score(docs[2], 2)

		if score1 != 1.0 {
			t.Errorf("expected score 1 for doc 1 (both conditions match), got %f", score1)
		}
		if score2 != 0.0 {
			t.Errorf("expected score 0 for doc 2 (neither condition match), got %f", score2)
		}
		if score3 != 0.0 {
			t.Errorf("expected score 0 for doc 3 (only one condition match), got %f", score3)
		}
	})

	t.Run("Filter Or combination yields 1 or 0", func(t *testing.T) {
		f, _ := filter.Parse([]any{"Or", []any{
			[]any{"category", "Eq", "premium"},
			[]any{"status", "Eq", "inactive"},
		}})
		clause := &RankClause{Type: RankClauseFilter, Filter: f}

		scorer := NewRankScorer(clause, configs)
		scorer.BuildIndexes(docs)

		// All docs should match (doc 1: premium, docs 2&3: inactive)
		for i, doc := range docs {
			score := scorer.Score(doc, uint32(i))
			if score != 1.0 {
				t.Errorf("expected score 1 for doc %d (Or condition), got %f", i+1, score)
			}
		}
	})

	t.Run("Filter NotEq yields 1 or 0", func(t *testing.T) {
		f, _ := filter.Parse([]any{"category", "NotEq", "premium"})
		clause := &RankClause{Type: RankClauseFilter, Filter: f}

		scorer := NewRankScorer(clause, configs)
		scorer.BuildIndexes(docs)

		score1 := scorer.Score(docs[0], 0) // premium - should be 0
		score2 := scorer.Score(docs[1], 1) // basic - should be 1
		score3 := scorer.Score(docs[2], 2) // premium - should be 0

		if score1 != 0.0 {
			t.Errorf("expected score 0 for premium (NotEq premium), got %f", score1)
		}
		if score2 != 1.0 {
			t.Errorf("expected score 1 for basic (NotEq premium), got %f", score2)
		}
		if score3 != 0.0 {
			t.Errorf("expected score 0 for premium (NotEq premium), got %f", score3)
		}
	})

	t.Run("Filter comparison operators yield 1 or 0", func(t *testing.T) {
		f, _ := filter.Parse([]any{"count", "Gt", int64(8)})
		clause := &RankClause{Type: RankClauseFilter, Filter: f}

		scorer := NewRankScorer(clause, configs)
		scorer.BuildIndexes(docs)

		// Doc 1: count=10 > 8 = match
		// Doc 2: count=5 > 8 = no match
		// Doc 3: count=20 > 8 = match
		score1 := scorer.Score(docs[0], 0)
		score2 := scorer.Score(docs[1], 1)
		score3 := scorer.Score(docs[2], 2)

		if score1 != 1.0 {
			t.Errorf("expected score 1 for doc 1 (10 > 8), got %f", score1)
		}
		if score2 != 0.0 {
			t.Errorf("expected score 0 for doc 2 (5 > 8 is false), got %f", score2)
		}
		if score3 != 1.0 {
			t.Errorf("expected score 1 for doc 3 (20 > 8), got %f", score3)
		}
	})

	t.Run("Filter with nil filter returns 0", func(t *testing.T) {
		clause := &RankClause{Type: RankClauseFilter, Filter: nil}

		scorer := NewRankScorer(clause, configs)
		scorer.BuildIndexes(docs)

		score := scorer.Score(docs[0], 0)
		if score != 0 {
			t.Errorf("expected score 0 for nil filter, got %f", score)
		}
	})

	t.Run("Filter boost in Sum adds 1 or 0", func(t *testing.T) {
		f, _ := filter.Parse([]any{"category", "Eq", "premium"})

		// Sum of BM25 + filter boost
		docsWithText := []*tail.Document{
			{
				ID:         document.NewU64ID(1),
				Attributes: map[string]any{"title": "hello world", "category": "premium"},
			},
			{
				ID:         document.NewU64ID(2),
				Attributes: map[string]any{"title": "hello world", "category": "basic"},
			},
		}

		configsWithFTS := map[string]*fts.Config{
			"title": fts.DefaultConfig(),
		}

		clause := &RankClause{
			Type: RankClauseSum,
			Clauses: []*RankClause{
				{Type: RankClauseBM25, Field: "title", QueryText: "hello"},
				{Type: RankClauseFilter, Filter: f},
			},
		}

		scorer := NewRankScorer(clause, configsWithFTS)
		scorer.BuildIndexes(docsWithText)

		score1 := scorer.Score(docsWithText[0], 0) // hello + premium
		score2 := scorer.Score(docsWithText[1], 1) // hello + basic

		// Score 1 should be BM25 + 1, Score 2 should be BM25 + 0
		if score1-score2 != 1.0 {
			t.Errorf("expected premium doc to score exactly 1 more than basic: score1=%f, score2=%f, diff=%f",
				score1, score2, score1-score2)
		}
	})

	t.Run("Filter boost in Product yields weight or 0", func(t *testing.T) {
		f, _ := filter.Parse([]any{"category", "Eq", "premium"})
		weight := 5.0

		clause := &RankClause{
			Type:   RankClauseProduct,
			Weight: weight,
			Clause: &RankClause{Type: RankClauseFilter, Filter: f},
		}

		scorer := NewRankScorer(clause, configs)
		scorer.BuildIndexes(docs)

		score1 := scorer.Score(docs[0], 0) // premium
		score2 := scorer.Score(docs[1], 1) // basic

		if score1 != weight {
			t.Errorf("expected score %f for matching filter (weight * 1), got %f", weight, score1)
		}
		if score2 != 0.0 {
			t.Errorf("expected score 0 for non-matching filter (weight * 0), got %f", score2)
		}
	})
}

// TestBM25ScoreComputationFormula tests the BM25 scoring formula behavior.
func TestBM25ScoreComputationFormula(t *testing.T) {
	configs := map[string]*fts.Config{
		"content": fts.DefaultConfig(),
	}

	t.Run("BM25 score increases with term frequency", func(t *testing.T) {
		docs := []*tail.Document{
			{
				ID:         document.NewU64ID(1),
				Attributes: map[string]any{"content": "apple"},
			},
			{
				ID:         document.NewU64ID(2),
				Attributes: map[string]any{"content": "apple apple"},
			},
			{
				ID:         document.NewU64ID(3),
				Attributes: map[string]any{"content": "apple apple apple"},
			},
		}

		clause := &RankClause{Type: RankClauseBM25, Field: "content", QueryText: "apple"}
		scorer := NewRankScorer(clause, configs)
		scorer.BuildIndexes(docs)

		score1 := scorer.Score(docs[0], 0)
		score2 := scorer.Score(docs[1], 1)
		score3 := scorer.Score(docs[2], 2)

		// Higher TF should lead to higher score (with saturation due to BM25 formula)
		if score2 <= score1 {
			t.Errorf("score should increase with TF: tf=1: %f, tf=2: %f", score1, score2)
		}
		if score3 <= score2 {
			t.Errorf("score should increase with TF: tf=2: %f, tf=3: %f", score2, score3)
		}
	})

	t.Run("BM25 score uses IDF - rare terms score higher", func(t *testing.T) {
		docs := []*tail.Document{
			{ID: document.NewU64ID(1), Attributes: map[string]any{"content": "common rare"}},
			{ID: document.NewU64ID(2), Attributes: map[string]any{"content": "common other"}},
			{ID: document.NewU64ID(3), Attributes: map[string]any{"content": "common another"}},
			{ID: document.NewU64ID(4), Attributes: map[string]any{"content": "common more"}},
		}

		clauseCommon := &RankClause{Type: RankClauseBM25, Field: "content", QueryText: "common"}
		clauseRare := &RankClause{Type: RankClauseBM25, Field: "content", QueryText: "rare"}

		scorerCommon := NewRankScorer(clauseCommon, configs)
		scorerCommon.BuildIndexes(docs)
		commonScore := scorerCommon.Score(docs[0], 0)

		scorerRare := NewRankScorer(clauseRare, configs)
		scorerRare.BuildIndexes(docs)
		rareScore := scorerRare.Score(docs[0], 0)

		// Rare term (appears in 1/4 docs) should score higher than common term (4/4 docs)
		if rareScore <= commonScore {
			t.Errorf("rare term should score higher: rare=%f, common=%f", rareScore, commonScore)
		}
	})

	t.Run("BM25 returns 0 for non-matching terms", func(t *testing.T) {
		docs := []*tail.Document{
			{ID: document.NewU64ID(1), Attributes: map[string]any{"content": "apple banana"}},
		}

		clause := &RankClause{Type: RankClauseBM25, Field: "content", QueryText: "nonexistent"}
		scorer := NewRankScorer(clause, configs)
		scorer.BuildIndexes(docs)

		score := scorer.Score(docs[0], 0)
		if score != 0 {
			t.Errorf("expected 0 for non-matching term, got %f", score)
		}
	})

	t.Run("BM25 returns 0 for empty query", func(t *testing.T) {
		docs := []*tail.Document{
			{ID: document.NewU64ID(1), Attributes: map[string]any{"content": "apple banana"}},
		}

		clause := &RankClause{Type: RankClauseBM25, Field: "content", QueryText: ""}
		scorer := NewRankScorer(clause, configs)
		scorer.BuildIndexes(docs)

		score := scorer.Score(docs[0], 0)
		if score != 0 {
			t.Errorf("expected 0 for empty query, got %f", score)
		}
	})

	t.Run("BM25 multi-term query sums individual term scores", func(t *testing.T) {
		docs := []*tail.Document{
			{ID: document.NewU64ID(1), Attributes: map[string]any{"content": "apple banana cherry"}},
			{ID: document.NewU64ID(2), Attributes: map[string]any{"content": "apple banana"}},
			{ID: document.NewU64ID(3), Attributes: map[string]any{"content": "apple"}},
		}

		clause := &RankClause{Type: RankClauseBM25, Field: "content", QueryText: "apple banana"}
		scorer := NewRankScorer(clause, configs)
		scorer.BuildIndexes(docs)

		// Doc 1 and 2 have both terms
		score1 := scorer.Score(docs[0], 0)
		score2 := scorer.Score(docs[1], 1)
		// Doc 3 has only apple
		score3 := scorer.Score(docs[2], 2)

		// Docs with both terms should score higher
		if score1 <= score3 || score2 <= score3 {
			t.Errorf("docs with more matching terms should score higher: two-term scores: %f, %f; one-term score: %f",
				score1, score2, score3)
		}
	})

	t.Run("BM25 returns 0 for missing field", func(t *testing.T) {
		docs := []*tail.Document{
			{ID: document.NewU64ID(1), Attributes: map[string]any{"title": "apple banana"}},
		}

		clause := &RankClause{Type: RankClauseBM25, Field: "content", QueryText: "apple"}
		scorer := NewRankScorer(clause, configs)
		scorer.BuildIndexes(docs)

		score := scorer.Score(docs[0], 0)
		if score != 0 {
			t.Errorf("expected 0 for missing field, got %f", score)
		}
	})

	t.Run("BM25 handles single document corpus", func(t *testing.T) {
		docs := []*tail.Document{
			{ID: document.NewU64ID(1), Attributes: map[string]any{"content": "apple banana"}},
		}

		clause := &RankClause{Type: RankClauseBM25, Field: "content", QueryText: "apple"}
		scorer := NewRankScorer(clause, configs)
		scorer.BuildIndexes(docs)

		score := scorer.Score(docs[0], 0)
		// IDF will be lower since term appears in all docs, but score should still be positive
		if score <= 0 {
			t.Errorf("expected positive score for matching term, got %f", score)
		}
	})

	t.Run("BM25 handles deleted documents", func(t *testing.T) {
		docs := []*tail.Document{
			{ID: document.NewU64ID(1), Attributes: map[string]any{"content": "apple"}, Deleted: true},
			{ID: document.NewU64ID(2), Attributes: map[string]any{"content": "apple"}},
		}

		clause := &RankClause{Type: RankClauseBM25, Field: "content", QueryText: "apple"}
		scorer := NewRankScorer(clause, configs)
		scorer.BuildIndexes(docs)

		// Deleted docs should not be indexed
		score1 := scorer.Score(docs[0], 0) // Deleted doc
		score2 := scorer.Score(docs[1], 1) // Normal doc

		// The deleted doc won't have an index entry, so score will be 0
		if score1 != 0 {
			t.Errorf("expected 0 for deleted doc (not indexed), got %f", score1)
		}
		if score2 <= 0 {
			t.Errorf("expected positive score for normal doc, got %f", score2)
		}
	})
}

// TestCompositeOperatorCombinations tests various combinations of operators.
func TestCompositeOperatorCombinations(t *testing.T) {
	docs := []*tail.Document{
		{
			ID: document.NewU64ID(1),
			Attributes: map[string]any{
				"title":    "search engine optimization",
				"content":  "learn how to optimize your website",
				"category": "tutorial",
			},
		},
		{
			ID: document.NewU64ID(2),
			Attributes: map[string]any{
				"title":    "database design patterns",
				"content":  "search for the best database patterns",
				"category": "reference",
			},
		},
	}

	configs := map[string]*fts.Config{
		"title":   fts.DefaultConfig(),
		"content": fts.DefaultConfig(),
	}

	t.Run("Sum of Max and Product", func(t *testing.T) {
		// Sum([Max([title BM25, content BM25]), Product(2, filter)])
		premiumFilter, _ := filter.Parse([]any{"category", "Eq", "tutorial"})

		clause := &RankClause{
			Type: RankClauseSum,
			Clauses: []*RankClause{
				{
					Type: RankClauseMax,
					Clauses: []*RankClause{
						{Type: RankClauseBM25, Field: "title", QueryText: "search"},
						{Type: RankClauseBM25, Field: "content", QueryText: "search"},
					},
				},
				{
					Type:   RankClauseProduct,
					Weight: 2.0,
					Clause: &RankClause{Type: RankClauseFilter, Filter: premiumFilter},
				},
			},
		}

		scorer := NewRankScorer(clause, configs)
		scorer.BuildIndexes(docs)

		score1 := scorer.Score(docs[0], 0) // Has "search" in title, is tutorial (+2)
		score2 := scorer.Score(docs[1], 1) // Has "search" in content, not tutorial (+0)

		if score1 <= score2 {
			t.Errorf("expected doc 1 (with filter boost) to score higher: doc1=%f, doc2=%f", score1, score2)
		}
	})

	t.Run("Max of Sums", func(t *testing.T) {
		// Max([Sum([title search, content search]), Sum([title database, content database])])
		clause := &RankClause{
			Type: RankClauseMax,
			Clauses: []*RankClause{
				{
					Type: RankClauseSum,
					Clauses: []*RankClause{
						{Type: RankClauseBM25, Field: "title", QueryText: "search"},
						{Type: RankClauseBM25, Field: "content", QueryText: "search"},
					},
				},
				{
					Type: RankClauseSum,
					Clauses: []*RankClause{
						{Type: RankClauseBM25, Field: "title", QueryText: "database"},
						{Type: RankClauseBM25, Field: "content", QueryText: "database"},
					},
				},
			},
		}

		scorer := NewRankScorer(clause, configs)
		scorer.BuildIndexes(docs)

		// Doc 1: higher score for "search" sum
		// Doc 2: higher score for "database" sum
		score1 := scorer.Score(docs[0], 0)
		score2 := scorer.Score(docs[1], 1)

		if score1 <= 0 || score2 <= 0 {
			t.Errorf("expected positive scores: doc1=%f, doc2=%f", score1, score2)
		}
	})

	t.Run("Product with Sum clause", func(t *testing.T) {
		weight := 1.5
		sumClause := &RankClause{
			Type: RankClauseSum,
			Clauses: []*RankClause{
				{Type: RankClauseBM25, Field: "title", QueryText: "search"},
				{Type: RankClauseBM25, Field: "content", QueryText: "optimize"},
			},
		}
		productClause := &RankClause{
			Type:   RankClauseProduct,
			Weight: weight,
			Clause: sumClause,
		}

		scorerSum := NewRankScorer(sumClause, configs)
		scorerSum.BuildIndexes(docs)
		sumScore := scorerSum.Score(docs[0], 0)

		scorerProduct := NewRankScorer(productClause, configs)
		scorerProduct.BuildIndexes(docs)
		productScore := scorerProduct.Score(docs[0], 0)

		expectedScore := weight * sumScore
		if math.Abs(productScore-expectedScore) > 0.0001 {
			t.Errorf("Product of Sum should multiply: got %f, expected %f", productScore, expectedScore)
		}
	})
}
