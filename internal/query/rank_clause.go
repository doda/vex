package query

import (
	"math"

	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/fts"
	"github.com/vexsearch/vex/internal/tail"
)

// RankClauseType indicates the type of rank clause.
type RankClauseType int

const (
	RankClauseBM25 RankClauseType = iota // ["field", "BM25", "query"]
	RankClauseSum                        // ["Sum", [...clauses...]]
	RankClauseMax                        // ["Max", [...clauses...]]
	RankClauseProduct                    // ["Product", weight, clause]
	RankClauseFilter                     // filter expression yields 1 or 0
)

// RankClause represents a parsed rank_by clause that can be composed.
// This supports BM25, Sum, Max, Product operators, and filter boosting.
type RankClause struct {
	Type RankClauseType

	// For BM25
	Field        string
	QueryText    string
	LastAsPrefix bool // Treat last token as prefix for typeahead

	// For Sum/Max
	Clauses []*RankClause

	// For Product
	Weight float64
	Clause *RankClause

	// For Filter
	Filter *filter.Filter
}

// ParseRankClause parses a compositional rank_by expression.
// Supports: BM25, Sum, Max, Product, and filter-based boosting.
func ParseRankClause(expr any) (*RankClause, error) {
	arr, ok := expr.([]any)
	if !ok {
		// Check if it's a filter-like expression (object/map)
		return tryParseFilterClause(expr)
	}

	if len(arr) < 1 {
		return nil, ErrInvalidRankBy
	}

	// Get the first element which is the operator/field name
	opOrField, ok := arr[0].(string)
	if !ok {
		// Could be a filter array
		return tryParseFilterClause(expr)
	}

	switch opOrField {
	case "Sum":
		return parseSumClause(arr)
	case "Max":
		return parseMaxClause(arr)
	case "Product":
		return parseProductClause(arr)
	default:
		// Check if this is a BM25 clause: ["field", "BM25", "query"]
		if len(arr) >= 3 {
			op, ok := arr[1].(string)
			if ok && op == "BM25" {
				return parseBM25Clause(arr)
			}
		}
		// Not a recognized composite operator, try as filter
		return tryParseFilterClause(expr)
	}
}

// parseSumClause parses ["Sum", [...clauses...]]
func parseSumClause(arr []any) (*RankClause, error) {
	if len(arr) != 2 {
		return nil, ErrInvalidRankBy
	}

	clausesArr, ok := arr[1].([]any)
	if !ok {
		return nil, ErrInvalidRankBy
	}

	clauses := make([]*RankClause, 0, len(clausesArr))
	for _, c := range clausesArr {
		parsed, err := ParseRankClause(c)
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, parsed)
	}

	return &RankClause{
		Type:    RankClauseSum,
		Clauses: clauses,
	}, nil
}

// parseMaxClause parses ["Max", [...clauses...]]
func parseMaxClause(arr []any) (*RankClause, error) {
	if len(arr) != 2 {
		return nil, ErrInvalidRankBy
	}

	clausesArr, ok := arr[1].([]any)
	if !ok {
		return nil, ErrInvalidRankBy
	}

	clauses := make([]*RankClause, 0, len(clausesArr))
	for _, c := range clausesArr {
		parsed, err := ParseRankClause(c)
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, parsed)
	}

	return &RankClause{
		Type:    RankClauseMax,
		Clauses: clauses,
	}, nil
}

// parseProductClause parses ["Product", weight, clause]
func parseProductClause(arr []any) (*RankClause, error) {
	if len(arr) != 3 {
		return nil, ErrInvalidRankBy
	}

	weight, err := toFloat64(arr[1])
	if err != nil {
		return nil, ErrInvalidRankBy
	}

	clause, err := ParseRankClause(arr[2])
	if err != nil {
		return nil, err
	}

	return &RankClause{
		Type:   RankClauseProduct,
		Weight: weight,
		Clause: clause,
	}, nil
}

// parseBM25Clause parses ["field", "BM25", "query"] or ["field", "BM25", {"query": "...", "last_as_prefix": true}]
func parseBM25Clause(arr []any) (*RankClause, error) {
	if len(arr) < 3 {
		return nil, ErrInvalidRankBy
	}

	field, ok := arr[0].(string)
	if !ok {
		return nil, ErrInvalidRankBy
	}

	var queryText string
	var lastAsPrefix bool

	switch v := arr[2].(type) {
	case string:
		queryText = v
	case map[string]any:
		q, ok := v["query"].(string)
		if !ok {
			return nil, ErrInvalidRankBy
		}
		queryText = q
		if lap, ok := v["last_as_prefix"].(bool); ok {
			lastAsPrefix = lap
		}
	default:
		return nil, ErrInvalidRankBy
	}

	return &RankClause{
		Type:         RankClauseBM25,
		Field:        field,
		QueryText:    queryText,
		LastAsPrefix: lastAsPrefix,
	}, nil
}

// tryParseFilterClause attempts to parse the expression as a filter.
// Filter clauses yield score 1 if matching, 0 otherwise.
func tryParseFilterClause(expr any) (*RankClause, error) {
	f, err := filter.Parse(expr)
	if err != nil {
		return nil, ErrInvalidRankBy
	}
	return &RankClause{
		Type:   RankClauseFilter,
		Filter: f,
	}, nil
}

// toFloat64 converts various numeric types to float64.
func toFloat64(v any) (float64, error) {
	switch n := v.(type) {
	case float64:
		return n, nil
	case float32:
		return float64(n), nil
	case int:
		return float64(n), nil
	case int64:
		return float64(n), nil
	case uint64:
		return float64(n), nil
	default:
		return 0, ErrInvalidRankBy
	}
}

// RankScorer scores documents using a parsed rank clause.
type RankScorer struct {
	clause *RankClause
	// Per-field FTS indexes built from documents for BM25 scoring.
	// key is field name, value is the FTS index.
	ftsIndexes map[string]*fts.Index
	// Map from internal FTS doc ID to tail document.
	docMaps map[string]map[uint32]*tail.Document
	// FTS configurations per field.
	ftsConfigs map[string]*fts.Config
}

// NewRankScorer creates a new scorer for the given clause.
func NewRankScorer(clause *RankClause, ftsConfigs map[string]*fts.Config) *RankScorer {
	return &RankScorer{
		clause:     clause,
		ftsIndexes: make(map[string]*fts.Index),
		docMaps:    make(map[string]map[uint32]*tail.Document),
		ftsConfigs: ftsConfigs,
	}
}

// BuildIndexes builds the FTS indexes needed for scoring from the documents.
// Must be called before Score() for BM25 clauses.
func (s *RankScorer) BuildIndexes(docs []*tail.Document) {
	// Find all BM25 fields used in the clause
	fields := s.collectBM25Fields(s.clause)

	for _, field := range fields {
		cfg := s.ftsConfigs[field]
		if cfg == nil {
			cfg = fts.DefaultConfig()
		}

		idx := fts.NewIndex(field, cfg)
		docMap := make(map[uint32]*tail.Document)

		for i, doc := range docs {
			if doc.Deleted {
				continue
			}
			text, ok := getDocTextValue(doc, field)
			if !ok || text == "" {
				continue
			}
			internalID := uint32(i)
			idx.AddDocument(internalID, text)
			docMap[internalID] = doc
		}

		s.ftsIndexes[field] = idx
		s.docMaps[field] = docMap
	}
}

// collectBM25Fields recursively finds all BM25 fields in a clause.
func (s *RankScorer) collectBM25Fields(c *RankClause) []string {
	if c == nil {
		return nil
	}

	var fields []string
	switch c.Type {
	case RankClauseBM25:
		fields = append(fields, c.Field)
	case RankClauseSum, RankClauseMax:
		for _, sub := range c.Clauses {
			fields = append(fields, s.collectBM25Fields(sub)...)
		}
	case RankClauseProduct:
		fields = append(fields, s.collectBM25Fields(c.Clause)...)
	}
	return fields
}

// getDocTextValue extracts a string field value from a document.
func getDocTextValue(doc *tail.Document, field string) (string, bool) {
	if doc.Attributes == nil {
		return "", false
	}
	val, ok := doc.Attributes[field]
	if !ok {
		return "", false
	}
	text, ok := val.(string)
	return text, ok
}

// Score computes the score for a document given the rank clause.
// internalDocID is the index into the docs slice passed to BuildIndexes.
func (s *RankScorer) Score(doc *tail.Document, internalDocID uint32) float64 {
	return s.scoreClause(s.clause, doc, internalDocID)
}

// scoreClause recursively computes the score for a clause.
func (s *RankScorer) scoreClause(c *RankClause, doc *tail.Document, internalDocID uint32) float64 {
	if c == nil {
		return 0
	}

	switch c.Type {
	case RankClauseBM25:
		return s.scoreBM25(c, doc, internalDocID)
	case RankClauseSum:
		return s.scoreSum(c, doc, internalDocID)
	case RankClauseMax:
		return s.scoreMax(c, doc, internalDocID)
	case RankClauseProduct:
		return s.scoreProduct(c, doc, internalDocID)
	case RankClauseFilter:
		return s.scoreFilter(c, doc)
	default:
		return 0
	}
}

// scoreBM25 computes BM25 score for a document.
// When LastAsPrefix is true, the last token is matched as a prefix and scores 1.0.
func (s *RankScorer) scoreBM25(c *RankClause, doc *tail.Document, internalDocID uint32) float64 {
	idx := s.ftsIndexes[c.Field]
	if idx == nil || idx.TotalDocs == 0 {
		return 0
	}

	cfg := s.ftsConfigs[c.Field]
	if cfg == nil {
		cfg = fts.DefaultConfig()
	}

	// Tokenize the query
	tokenizer := fts.NewTokenizer(cfg)
	queryTokens := tokenizer.Tokenize(c.QueryText)
	if cfg.RemoveStopwords {
		queryTokens = fts.RemoveStopwords(queryTokens)
	}

	if len(queryTokens) == 0 {
		return 0
	}

	return computeBM25ScoreWithPrefix(idx, internalDocID, queryTokens, c.LastAsPrefix, cfg)
}

// scoreSum sums the scores from all sub-clauses.
func (s *RankScorer) scoreSum(c *RankClause, doc *tail.Document, internalDocID uint32) float64 {
	var sum float64
	for _, sub := range c.Clauses {
		sum += s.scoreClause(sub, doc, internalDocID)
	}
	return sum
}

// scoreMax takes the maximum score from all sub-clauses.
func (s *RankScorer) scoreMax(c *RankClause, doc *tail.Document, internalDocID uint32) float64 {
	maxScore := math.Inf(-1)
	hasAny := false
	for _, sub := range c.Clauses {
		score := s.scoreClause(sub, doc, internalDocID)
		if score > maxScore {
			maxScore = score
		}
		hasAny = true
	}
	if !hasAny {
		return 0
	}
	return maxScore
}

// scoreProduct multiplies the weight by the sub-clause score.
func (s *RankScorer) scoreProduct(c *RankClause, doc *tail.Document, internalDocID uint32) float64 {
	subScore := s.scoreClause(c.Clause, doc, internalDocID)
	return c.Weight * subScore
}

// scoreFilter returns 1 if the filter matches, 0 otherwise.
func (s *RankScorer) scoreFilter(c *RankClause, doc *tail.Document) float64 {
	if c.Filter == nil {
		return 0
	}
	if c.Filter.Eval(filter.Document(doc.Attributes)) {
		return 1
	}
	return 0
}

// IsCompositeClause returns true if the clause uses compositional operators.
func IsCompositeClause(c *RankClause) bool {
	if c == nil {
		return false
	}
	switch c.Type {
	case RankClauseSum, RankClauseMax, RankClauseProduct:
		return true
	case RankClauseFilter:
		return true
	default:
		return false
	}
}
