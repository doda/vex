package vector

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
)

var (
	// ErrInvalidIVFFormat is returned when IVF data has an invalid format.
	ErrInvalidIVFFormat = errors.New("invalid IVF index format")

	// ErrEmptyIndex is returned when attempting to query an empty index.
	ErrEmptyIndex = errors.New("empty IVF index")

	// ErrNoVectors is returned when attempting to build an index with no vectors.
	ErrNoVectors = errors.New("no vectors provided for index building")
)

const (
	// IVFMagic is the magic number for IVF index files.
	IVFMagic = uint32(0x56455849) // "VEXI"

	// IVFVersion is the current IVF index format version.
	IVFVersion = uint32(1)

	// DefaultNClusters is the default number of clusters for IVF.
	// Set based on typical guidance: sqrt(n) clusters for n vectors.
	DefaultNClusters = 256

	// DefaultNProbe is the default number of clusters to probe during search.
	DefaultNProbe = 16
)

// IVFIndex represents an IVF (Inverted File) index for approximate nearest neighbor search.
// It partitions vectors into clusters based on centroids and enables efficient search
// by only examining vectors in the nearest clusters.
type IVFIndex struct {
	// Dims is the number of dimensions in each vector.
	Dims int

	// Metric is the distance metric used for similarity calculations.
	Metric DistanceMetric

	// NClusters is the number of clusters (centroids) in the index.
	NClusters int

	// Centroids are the cluster center vectors.
	// Layout: [NClusters][Dims]float32
	Centroids []float32

	// ClusterOffsets maps each cluster to its offset and length in the packed data.
	// Layout: [NClusters] ClusterOffset
	ClusterOffsets []ClusterOffset

	// ClusterData contains packed cluster data for all clusters.
	// Each cluster contains: [doc_id: uint64, vector: [dims]float32] pairs
	ClusterData []byte
}

// ClusterOffset describes the location and size of a cluster in the packed data.
type ClusterOffset struct {
	// Offset is the byte offset into ClusterData where this cluster starts.
	Offset uint64

	// Length is the byte length of this cluster's data.
	Length uint64

	// DocCount is the number of documents in this cluster.
	DocCount uint32
}

// IVFSearchResult represents a single search result.
type IVFSearchResult struct {
	// DocID is the document ID.
	DocID uint64

	// Distance is the distance from the query vector.
	Distance float32

	// ClusterID is the cluster this document belongs to.
	ClusterID int
}

// IVFBuilder builds an IVF index from a set of vectors.
type IVFBuilder struct {
	dims       int
	metric     DistanceMetric
	nClusters  int
	maxIter    int
	tolerance  float64
	vectors    []float32
	docIDs     []uint64
}

// NewIVFBuilder creates a new IVF index builder.
func NewIVFBuilder(dims int, metric DistanceMetric, nClusters int) *IVFBuilder {
	if nClusters <= 0 {
		nClusters = DefaultNClusters
	}
	return &IVFBuilder{
		dims:      dims,
		metric:    metric,
		nClusters: nClusters,
		maxIter:   20,
		tolerance: 0.001,
		vectors:   make([]float32, 0),
		docIDs:    make([]uint64, 0),
	}
}

// SetMaxIterations sets the maximum number of k-means iterations.
func (b *IVFBuilder) SetMaxIterations(maxIter int) {
	if maxIter > 0 {
		b.maxIter = maxIter
	}
}

// SetTolerance sets the convergence tolerance for k-means.
func (b *IVFBuilder) SetTolerance(tolerance float64) {
	if tolerance > 0 {
		b.tolerance = tolerance
	}
}

// AddVector adds a vector with its document ID to the builder.
func (b *IVFBuilder) AddVector(docID uint64, vec []float32) error {
	if len(vec) != b.dims {
		return fmt.Errorf("vector dimension mismatch: expected %d, got %d", b.dims, len(vec))
	}
	b.vectors = append(b.vectors, vec...)
	b.docIDs = append(b.docIDs, docID)
	return nil
}

// AddVectors adds multiple vectors with their document IDs.
func (b *IVFBuilder) AddVectors(docIDs []uint64, vectors [][]float32) error {
	if len(docIDs) != len(vectors) {
		return fmt.Errorf("docIDs and vectors length mismatch: %d vs %d", len(docIDs), len(vectors))
	}
	for i, vec := range vectors {
		if err := b.AddVector(docIDs[i], vec); err != nil {
			return err
		}
	}
	return nil
}

// Count returns the number of vectors added to the builder.
func (b *IVFBuilder) Count() int {
	return len(b.docIDs)
}

// Build builds the IVF index from the added vectors.
func (b *IVFBuilder) Build() (*IVFIndex, error) {
	n := len(b.docIDs)
	if n == 0 {
		return nil, ErrNoVectors
	}

	// Adjust number of clusters if we have fewer vectors
	nClusters := b.nClusters
	if n < nClusters {
		nClusters = n
	}

	// Run k-means clustering
	centroids, assignments, err := kmeans(b.vectors, b.dims, nClusters, b.maxIter, b.tolerance, b.metric)
	if err != nil {
		return nil, fmt.Errorf("k-means clustering failed: %w", err)
	}

	// Build cluster data
	clusterOffsets, clusterData := b.buildClusterData(nClusters, assignments)

	return &IVFIndex{
		Dims:           b.dims,
		Metric:         b.metric,
		NClusters:      nClusters,
		Centroids:      centroids,
		ClusterOffsets: clusterOffsets,
		ClusterData:    clusterData,
	}, nil
}

// buildClusterData packs vectors into cluster format.
func (b *IVFBuilder) buildClusterData(nClusters int, assignments []int) ([]ClusterOffset, []byte) {
	// Group vectors by cluster
	clusterVecs := make([][][]float32, nClusters)
	clusterDocIDs := make([][]uint64, nClusters)
	for i := range nClusters {
		clusterVecs[i] = make([][]float32, 0)
		clusterDocIDs[i] = make([]uint64, 0)
	}

	n := len(b.docIDs)
	for i := 0; i < n; i++ {
		cluster := assignments[i]
		vec := b.vectors[i*b.dims : (i+1)*b.dims]
		clusterVecs[cluster] = append(clusterVecs[cluster], vec)
		clusterDocIDs[cluster] = append(clusterDocIDs[cluster], b.docIDs[i])
	}

	// Build packed cluster data
	var buf bytes.Buffer
	offsets := make([]ClusterOffset, nClusters)

	for i := 0; i < nClusters; i++ {
		offset := uint64(buf.Len())
		docCount := len(clusterDocIDs[i])

		// Write each (docID, vector) pair
		for j := 0; j < docCount; j++ {
			binary.Write(&buf, binary.LittleEndian, clusterDocIDs[i][j])
			for _, f := range clusterVecs[i][j] {
				binary.Write(&buf, binary.LittleEndian, f)
			}
		}

		length := uint64(buf.Len()) - offset
		offsets[i] = ClusterOffset{
			Offset:   offset,
			Length:   length,
			DocCount: uint32(docCount),
		}
	}

	return offsets, buf.Bytes()
}

// Search performs approximate nearest neighbor search.
func (idx *IVFIndex) Search(query []float32, topK, nProbe int) ([]IVFSearchResult, error) {
	if len(query) != idx.Dims {
		return nil, fmt.Errorf("query dimension mismatch: expected %d, got %d", idx.Dims, len(query))
	}
	if topK <= 0 {
		return nil, fmt.Errorf("topK must be positive, got %d", topK)
	}
	if idx.NClusters == 0 {
		return nil, ErrEmptyIndex
	}
	if nProbe <= 0 {
		nProbe = DefaultNProbe
	}
	if nProbe > idx.NClusters {
		nProbe = idx.NClusters
	}

	// Find nearest centroids
	nearestClusters := idx.findNearestCentroids(query, nProbe)

	// Search within selected clusters
	results := make([]IVFSearchResult, 0)
	for _, clusterID := range nearestClusters {
		clusterResults := idx.searchCluster(query, clusterID)
		results = append(results, clusterResults...)
	}

	// Sort by distance and return top K
	sort.Slice(results, func(i, j int) bool {
		return results[i].Distance < results[j].Distance
	})

	if len(results) > topK {
		results = results[:topK]
	}

	return results, nil
}

// findNearestCentroids returns the indices of the nProbe nearest centroids.
func (idx *IVFIndex) findNearestCentroids(query []float32, nProbe int) []int {
	type centroidDist struct {
		idx  int
		dist float32
	}

	distances := make([]centroidDist, idx.NClusters)
	for i := 0; i < idx.NClusters; i++ {
		centroid := idx.Centroids[i*idx.Dims : (i+1)*idx.Dims]
		dist := computeDistance(query, centroid, idx.Metric)
		distances[i] = centroidDist{idx: i, dist: dist}
	}

	sort.Slice(distances, func(i, j int) bool {
		return distances[i].dist < distances[j].dist
	})

	result := make([]int, nProbe)
	for i := 0; i < nProbe; i++ {
		result[i] = distances[i].idx
	}
	return result
}

// searchCluster searches a single cluster for nearest neighbors.
func (idx *IVFIndex) searchCluster(query []float32, clusterID int) []IVFSearchResult {
	offset := idx.ClusterOffsets[clusterID]
	if offset.DocCount == 0 {
		return nil
	}

	results := make([]IVFSearchResult, 0, offset.DocCount)
	data := idx.ClusterData[offset.Offset : offset.Offset+offset.Length]

	// Each entry is: docID (8 bytes) + vector (dims * 4 bytes)
	entrySize := 8 + idx.Dims*4
	reader := bytes.NewReader(data)

	for i := 0; i < int(offset.DocCount); i++ {
		var docID uint64
		binary.Read(reader, binary.LittleEndian, &docID)

		vec := make([]float32, idx.Dims)
		for j := 0; j < idx.Dims; j++ {
			binary.Read(reader, binary.LittleEndian, &vec[j])
		}

		// Skip if we've consumed the expected entry
		if int(offset.DocCount)*entrySize > int(offset.Length) {
			break
		}

		dist := computeDistance(query, vec, idx.Metric)
		results = append(results, IVFSearchResult{
			DocID:     docID,
			Distance:  dist,
			ClusterID: clusterID,
		})
	}

	return results
}

// computeDistance calculates the distance between two vectors using the specified metric.
func computeDistance(a, b []float32, metric DistanceMetric) float32 {
	switch metric {
	case MetricCosineDistance:
		return cosineDistance(a, b)
	case MetricEuclideanSquared:
		return euclideanSquared(a, b)
	case MetricDotProduct:
		return -dotProduct(a, b) // Negative because we want smaller = more similar
	default:
		return cosineDistance(a, b)
	}
}

// cosineDistance computes 1 - cosine_similarity(a, b).
func cosineDistance(a, b []float32) float32 {
	var dot, normA, normB float32
	for i := range a {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	if normA == 0 || normB == 0 {
		return 1.0
	}
	cosine := dot / (float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB))))
	return 1.0 - cosine
}

// euclideanSquared computes sum((a[i] - b[i])^2).
func euclideanSquared(a, b []float32) float32 {
	var sum float32
	for i := range a {
		d := a[i] - b[i]
		sum += d * d
	}
	return sum
}

// dotProduct computes sum(a[i] * b[i]).
func dotProduct(a, b []float32) float32 {
	var sum float32
	for i := range a {
		sum += a[i] * b[i]
	}
	return sum
}

// === File format serialization ===

// WriteCentroidsFile writes centroids to the specified format.
// Format: [magic: u32][version: u32][dims: u32][n_clusters: u32][metric: u8 (padded to 4)][centroids: f32...]
func (idx *IVFIndex) WriteCentroidsFile(w io.Writer) error {
	// Magic
	if err := binary.Write(w, binary.LittleEndian, IVFMagic); err != nil {
		return err
	}
	// Version
	if err := binary.Write(w, binary.LittleEndian, IVFVersion); err != nil {
		return err
	}
	// Dims
	if err := binary.Write(w, binary.LittleEndian, uint32(idx.Dims)); err != nil {
		return err
	}
	// NClusters
	if err := binary.Write(w, binary.LittleEndian, uint32(idx.NClusters)); err != nil {
		return err
	}
	// Metric (as u8 with 3 bytes padding)
	metricByte := byte(0)
	switch idx.Metric {
	case MetricCosineDistance:
		metricByte = 0
	case MetricEuclideanSquared:
		metricByte = 1
	case MetricDotProduct:
		metricByte = 2
	}
	if err := binary.Write(w, binary.LittleEndian, [4]byte{metricByte, 0, 0, 0}); err != nil {
		return err
	}
	// Centroids
	for _, f := range idx.Centroids {
		if err := binary.Write(w, binary.LittleEndian, f); err != nil {
			return err
		}
	}
	return nil
}

// WriteClusterOffsetsFile writes cluster offsets to the specified format.
// Format: [magic: u32][version: u32][n_clusters: u32][offsets: (offset: u64, length: u64, doc_count: u32, padding: u32)...]
func (idx *IVFIndex) WriteClusterOffsetsFile(w io.Writer) error {
	// Magic
	if err := binary.Write(w, binary.LittleEndian, IVFMagic); err != nil {
		return err
	}
	// Version
	if err := binary.Write(w, binary.LittleEndian, IVFVersion); err != nil {
		return err
	}
	// NClusters
	if err := binary.Write(w, binary.LittleEndian, uint32(idx.NClusters)); err != nil {
		return err
	}
	// Offsets
	for _, o := range idx.ClusterOffsets {
		if err := binary.Write(w, binary.LittleEndian, o.Offset); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, o.Length); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, o.DocCount); err != nil {
			return err
		}
		// Padding for alignment
		if err := binary.Write(w, binary.LittleEndian, uint32(0)); err != nil {
			return err
		}
	}
	return nil
}

// WriteClusterDataFile writes packed cluster data.
// Format: raw bytes (no header, the offset file provides structure)
func (idx *IVFIndex) WriteClusterDataFile(w io.Writer) error {
	_, err := w.Write(idx.ClusterData)
	return err
}

// ReadCentroidsFile reads centroids from the specified format.
func ReadCentroidsFile(r io.Reader) (dims, nClusters int, metric DistanceMetric, centroids []float32, err error) {
	// Magic
	var magic uint32
	if err := binary.Read(r, binary.LittleEndian, &magic); err != nil {
		return 0, 0, "", nil, err
	}
	if magic != IVFMagic {
		return 0, 0, "", nil, fmt.Errorf("%w: invalid magic number 0x%X", ErrInvalidIVFFormat, magic)
	}

	// Version
	var version uint32
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return 0, 0, "", nil, err
	}
	if version != IVFVersion {
		return 0, 0, "", nil, fmt.Errorf("%w: unsupported version %d", ErrInvalidIVFFormat, version)
	}

	// Dims
	var dimsU32 uint32
	if err := binary.Read(r, binary.LittleEndian, &dimsU32); err != nil {
		return 0, 0, "", nil, err
	}

	// NClusters
	var nClustersU32 uint32
	if err := binary.Read(r, binary.LittleEndian, &nClustersU32); err != nil {
		return 0, 0, "", nil, err
	}

	// Metric
	var metricBytes [4]byte
	if err := binary.Read(r, binary.LittleEndian, &metricBytes); err != nil {
		return 0, 0, "", nil, err
	}
	switch metricBytes[0] {
	case 0:
		metric = MetricCosineDistance
	case 1:
		metric = MetricEuclideanSquared
	case 2:
		metric = MetricDotProduct
	default:
		return 0, 0, "", nil, fmt.Errorf("%w: unknown metric %d", ErrInvalidIVFFormat, metricBytes[0])
	}

	// Centroids
	centroidCount := int(dimsU32) * int(nClustersU32)
	centroids = make([]float32, centroidCount)
	for i := 0; i < centroidCount; i++ {
		if err := binary.Read(r, binary.LittleEndian, &centroids[i]); err != nil {
			return 0, 0, "", nil, err
		}
	}

	return int(dimsU32), int(nClustersU32), metric, centroids, nil
}

// ReadClusterOffsetsFile reads cluster offsets from the specified format.
func ReadClusterOffsetsFile(r io.Reader) ([]ClusterOffset, error) {
	// Magic
	var magic uint32
	if err := binary.Read(r, binary.LittleEndian, &magic); err != nil {
		return nil, err
	}
	if magic != IVFMagic {
		return nil, fmt.Errorf("%w: invalid magic number 0x%X", ErrInvalidIVFFormat, magic)
	}

	// Version
	var version uint32
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return nil, err
	}
	if version != IVFVersion {
		return nil, fmt.Errorf("%w: unsupported version %d", ErrInvalidIVFFormat, version)
	}

	// NClusters
	var nClusters uint32
	if err := binary.Read(r, binary.LittleEndian, &nClusters); err != nil {
		return nil, err
	}

	// Offsets
	offsets := make([]ClusterOffset, nClusters)
	for i := 0; i < int(nClusters); i++ {
		if err := binary.Read(r, binary.LittleEndian, &offsets[i].Offset); err != nil {
			return nil, err
		}
		if err := binary.Read(r, binary.LittleEndian, &offsets[i].Length); err != nil {
			return nil, err
		}
		if err := binary.Read(r, binary.LittleEndian, &offsets[i].DocCount); err != nil {
			return nil, err
		}
		var padding uint32
		if err := binary.Read(r, binary.LittleEndian, &padding); err != nil {
			return nil, err
		}
	}

	return offsets, nil
}

// LoadIVFIndex loads an IVF index from its component parts.
func LoadIVFIndex(centroids []float32, dims, nClusters int, metric DistanceMetric,
	offsets []ClusterOffset, clusterData []byte) *IVFIndex {
	return &IVFIndex{
		Dims:           dims,
		Metric:         metric,
		NClusters:      nClusters,
		Centroids:      centroids,
		ClusterOffsets: offsets,
		ClusterData:    clusterData,
	}
}

// GetCentroidsBytes returns the raw bytes for centroids.
func (idx *IVFIndex) GetCentroidsBytes() []byte {
	var buf bytes.Buffer
	idx.WriteCentroidsFile(&buf)
	return buf.Bytes()
}

// GetClusterOffsetsBytes returns the raw bytes for cluster offsets.
func (idx *IVFIndex) GetClusterOffsetsBytes() []byte {
	var buf bytes.Buffer
	idx.WriteClusterOffsetsFile(&buf)
	return buf.Bytes()
}

// GetClusterDataBytes returns the raw bytes for cluster data.
func (idx *IVFIndex) GetClusterDataBytes() []byte {
	return idx.ClusterData
}

// TotalVectorCount returns the total number of vectors in the index.
func (idx *IVFIndex) TotalVectorCount() int {
	var count int
	for _, o := range idx.ClusterOffsets {
		count += int(o.DocCount)
	}
	return count
}

// GetClusterVectors reads and returns all vectors from a specific cluster.
func (idx *IVFIndex) GetClusterVectors(clusterID int) (docIDs []uint64, vectors [][]float32, err error) {
	if clusterID < 0 || clusterID >= idx.NClusters {
		return nil, nil, fmt.Errorf("cluster ID %d out of range [0, %d)", clusterID, idx.NClusters)
	}

	offset := idx.ClusterOffsets[clusterID]
	if offset.DocCount == 0 {
		return []uint64{}, [][]float32{}, nil
	}

	data := idx.ClusterData[offset.Offset : offset.Offset+offset.Length]
	reader := bytes.NewReader(data)

	docIDs = make([]uint64, 0, offset.DocCount)
	vectors = make([][]float32, 0, offset.DocCount)

	for i := 0; i < int(offset.DocCount); i++ {
		var docID uint64
		if err := binary.Read(reader, binary.LittleEndian, &docID); err != nil {
			return nil, nil, err
		}

		vec := make([]float32, idx.Dims)
		for j := 0; j < idx.Dims; j++ {
			if err := binary.Read(reader, binary.LittleEndian, &vec[j]); err != nil {
				return nil, nil, err
			}
		}

		docIDs = append(docIDs, docID)
		vectors = append(vectors, vec)
	}

	return docIDs, vectors, nil
}
