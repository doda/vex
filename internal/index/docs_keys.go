package index

import "strings"

// DocsOffsetsKey derives the docs offsets object key from the docs key.
func DocsOffsetsKey(docsKey string) string {
	if strings.HasSuffix(docsKey, "/docs.col.zst") {
		return strings.TrimSuffix(docsKey, "/docs.col.zst") + "/docs.offsets.bin"
	}
	if strings.HasSuffix(docsKey, "docs.col.zst") {
		return strings.TrimSuffix(docsKey, "docs.col.zst") + "docs.offsets.bin"
	}
	return ""
}

// DocsIDMapKey derives the doc ID map object key from the docs key.
func DocsIDMapKey(docsKey string) string {
	if strings.HasSuffix(docsKey, "/docs.col.zst") {
		return strings.TrimSuffix(docsKey, "/docs.col.zst") + "/docs.idmap.bin"
	}
	if strings.HasSuffix(docsKey, "docs.col.zst") {
		return strings.TrimSuffix(docsKey, "docs.col.zst") + "docs.idmap.bin"
	}
	return ""
}
