package query

import (
	"encoding/json"
	"fmt"
)

// ParseQueryRequest parses a query request from a JSON body.
func ParseQueryRequest(body map[string]any) (*QueryRequest, error) {
	req := &QueryRequest{
		Limit: DefaultLimit,
	}

	// Parse rank_by
	if v, ok := body["rank_by"]; ok {
		req.RankBy = v
	}

	// Parse filters
	if v, ok := body["filters"]; ok {
		req.Filters = v
	}
	// Also accept "filter" as an alias
	if v, ok := body["filter"]; ok && req.Filters == nil {
		req.Filters = v
	}

	// Parse include_attributes
	if v, ok := body["include_attributes"]; ok {
		attrs, err := parseStringSlice(v)
		if err != nil {
			return nil, fmt.Errorf("include_attributes: %w", err)
		}
		req.IncludeAttributes = attrs
	}

	// Parse exclude_attributes
	if v, ok := body["exclude_attributes"]; ok {
		attrs, err := parseStringSlice(v)
		if err != nil {
			return nil, fmt.Errorf("exclude_attributes: %w", err)
		}
		req.ExcludeAttributes = attrs
	}

	// Parse limit (or top_k alias)
	if v, ok := body["limit"]; ok {
		limit, err := parseIntValue(v)
		if err != nil {
			return nil, fmt.Errorf("limit: %w", err)
		}
		req.Limit = limit
	}
	if v, ok := body["top_k"]; ok {
		limit, err := parseIntValue(v)
		if err != nil {
			return nil, fmt.Errorf("top_k: %w", err)
		}
		req.Limit = limit
	}

	// Parse per (diversification for order-by queries)
	if v, ok := body["per"]; ok {
		perAttrs, err := parseStringSlice(v)
		if err != nil {
			return nil, fmt.Errorf("per: %w", err)
		}
		req.Per = perAttrs
	}

	// Parse aggregate_by
	if v, ok := body["aggregate_by"]; ok {
		req.AggregateBy = v
	}

	// Parse group_by
	if v, ok := body["group_by"]; ok {
		req.GroupBy = v
	}

	// Parse vector_encoding
	if v, ok := body["vector_encoding"].(string); ok {
		req.VectorEncoding = v
	}

	// Parse consistency
	if v, ok := body["consistency"].(string); ok {
		req.Consistency = v
	}

	return req, nil
}

// parseStringSlice parses a value as a string slice.
func parseStringSlice(v any) ([]string, error) {
	switch val := v.(type) {
	case []string:
		return val, nil
	case []any:
		result := make([]string, 0, len(val))
		for i, item := range val {
			s, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("element %d is not a string", i)
			}
			result = append(result, s)
		}
		return result, nil
	default:
		return nil, fmt.Errorf("expected array of strings")
	}
}

// parseIntValue parses a value as an integer.
func parseIntValue(v any) (int, error) {
	switch val := v.(type) {
	case int:
		return val, nil
	case int64:
		return int(val), nil
	case float64:
		return int(val), nil
	case json.Number:
		i, err := val.Int64()
		if err != nil {
			return 0, err
		}
		return int(i), nil
	default:
		return 0, fmt.Errorf("expected integer")
	}
}

// RowToJSON converts a Row to a JSON-serializable map.
func RowToJSON(row Row) map[string]any {
	result := make(map[string]any)
	result["id"] = row.ID
	if row.Dist != nil {
		result["$dist"] = *row.Dist
	}
	// Merge attributes
	for k, v := range row.Attributes {
		result[k] = v
	}
	return result
}
