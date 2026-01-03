package vector

import (
	"math"
	"math/rand"
)

// kmeans performs k-means clustering on a set of vectors.
// Returns centroids and cluster assignments for each vector.
func kmeans(vectors []float32, dims, k, maxIter int, tolerance float64, metric DistanceMetric) ([]float32, []int, error) {
	n := len(vectors) / dims
	if n == 0 {
		return nil, nil, ErrNoVectors
	}
	if k > n {
		k = n
	}

	// Initialize centroids using k-means++ style initialization
	centroids := initCentroids(vectors, dims, k, metric)
	assignments := make([]int, n)

	var prevCentroids []float32
	for iter := 0; iter < maxIter; iter++ {
		// Assignment step: assign each vector to nearest centroid
		for i := 0; i < n; i++ {
			vec := vectors[i*dims : (i+1)*dims]
			minDist := float32(math.MaxFloat32)
			minCluster := 0
			for j := 0; j < k; j++ {
				centroid := centroids[j*dims : (j+1)*dims]
				dist := computeDistance(vec, centroid, metric)
				if dist < minDist {
					minDist = dist
					minCluster = j
				}
			}
			assignments[i] = minCluster
		}

		// Update step: recompute centroids
		prevCentroids = make([]float32, len(centroids))
		copy(prevCentroids, centroids)

		newCentroids := make([]float32, k*dims)
		clusterSizes := make([]int, k)

		for i := 0; i < n; i++ {
			cluster := assignments[i]
			clusterSizes[cluster]++
			for d := 0; d < dims; d++ {
				newCentroids[cluster*dims+d] += vectors[i*dims+d]
			}
		}

		// Average the sums to get new centroids
		for j := 0; j < k; j++ {
			if clusterSizes[j] > 0 {
				for d := 0; d < dims; d++ {
					centroids[j*dims+d] = newCentroids[j*dims+d] / float32(clusterSizes[j])
				}
			}
			// If cluster is empty, reinitialize randomly
			if clusterSizes[j] == 0 {
				randIdx := rand.Intn(n)
				copy(centroids[j*dims:(j+1)*dims], vectors[randIdx*dims:(randIdx+1)*dims])
			}
		}

		// Normalize centroids for cosine distance
		if metric == MetricCosineDistance {
			normalizeCentroids(centroids, dims, k)
		}

		// Check for convergence
		if centroidChange(prevCentroids, centroids, dims, k) < tolerance {
			break
		}
	}

	return centroids, assignments, nil
}

// initCentroids initializes centroids using k-means++ style initialization.
// This provides better initial centroids than random selection.
func initCentroids(vectors []float32, dims, k int, metric DistanceMetric) []float32 {
	n := len(vectors) / dims
	centroids := make([]float32, k*dims)

	// Choose first centroid randomly
	idx := rand.Intn(n)
	copy(centroids[0:dims], vectors[idx*dims:(idx+1)*dims])

	// Choose remaining centroids with probability proportional to distance squared
	distances := make([]float32, n)
	for i := 1; i < k; i++ {
		// Update distances to nearest centroid
		var totalDist float32
		for j := 0; j < n; j++ {
			vec := vectors[j*dims : (j+1)*dims]
			minDist := float32(math.MaxFloat32)
			for c := 0; c < i; c++ {
				centroid := centroids[c*dims : (c+1)*dims]
				dist := computeDistance(vec, centroid, metric)
				if dist < minDist {
					minDist = dist
				}
			}
			distances[j] = minDist * minDist // Weight by distance squared
			totalDist += distances[j]
		}

		// Sample next centroid
		if totalDist == 0 {
			idx = rand.Intn(n)
		} else {
			target := rand.Float32() * totalDist
			var cumulative float32
			idx = n - 1 // Default to last if not found
			for j := 0; j < n; j++ {
				cumulative += distances[j]
				if cumulative >= target {
					idx = j
					break
				}
			}
		}

		copy(centroids[i*dims:(i+1)*dims], vectors[idx*dims:(idx+1)*dims])
	}

	// Normalize for cosine distance
	if metric == MetricCosineDistance {
		normalizeCentroids(centroids, dims, k)
	}

	return centroids
}

// normalizeCentroids normalizes each centroid to unit length.
func normalizeCentroids(centroids []float32, dims, k int) {
	for i := 0; i < k; i++ {
		var norm float32
		for d := 0; d < dims; d++ {
			norm += centroids[i*dims+d] * centroids[i*dims+d]
		}
		if norm > 0 {
			norm = float32(math.Sqrt(float64(norm)))
			for d := 0; d < dims; d++ {
				centroids[i*dims+d] /= norm
			}
		}
	}
}

// centroidChange calculates the total change in centroids between iterations.
func centroidChange(prev, curr []float32, dims, k int) float64 {
	var totalChange float64
	for i := 0; i < k*dims; i++ {
		d := float64(curr[i] - prev[i])
		totalChange += d * d
	}
	return math.Sqrt(totalChange / float64(k))
}

// splitOversizedClusters splits clusters that exceed maxSize into smaller clusters.
// This ensures balanced cluster sizes for efficient search.
func splitOversizedClusters(vectors []float32, dims int, centroids []float32, assignments []int, nClusters, maxSize int, metric DistanceMetric) ([]float32, []int, int) {
	n := len(assignments)

	// Count cluster sizes
	clusterSizes := make([]int, nClusters)
	for _, c := range assignments {
		clusterSizes[c]++
	}

	// Find oversized clusters
	var oversized []int
	for i, size := range clusterSizes {
		if size > maxSize {
			oversized = append(oversized, i)
		}
	}

	if len(oversized) == 0 {
		return centroids, assignments, nClusters
	}

	// Split each oversized cluster
	newCentroids := make([]float32, len(centroids))
	copy(newCentroids, centroids)
	newAssignments := make([]int, n)
	copy(newAssignments, assignments)
	newNClusters := nClusters

	for _, clusterID := range oversized {
		// Collect vectors in this cluster
		var clusterVecIndices []int
		for i, c := range newAssignments {
			if c == clusterID {
				clusterVecIndices = append(clusterVecIndices, i)
			}
		}

		if len(clusterVecIndices) <= maxSize {
			continue // Already small enough (may have been shrunk by previous splits)
		}

		// Extract vectors for this cluster
		clusterVectors := make([]float32, len(clusterVecIndices)*dims)
		for i, idx := range clusterVecIndices {
			copy(clusterVectors[i*dims:(i+1)*dims], vectors[idx*dims:(idx+1)*dims])
		}

		// Split into 2 sub-clusters using k-means
		numSplits := 2
		for len(clusterVecIndices)/numSplits > maxSize && numSplits < 16 {
			numSplits *= 2 // Split into more clusters if still too large
		}

		subCentroids, subAssignments, err := kmeans(clusterVectors, dims, numSplits, 20, 0.001, metric)
		if err != nil {
			continue // Skip splitting on error
		}

		// First sub-cluster keeps the original cluster ID
		// Additional sub-clusters get new IDs
		for i, subCluster := range subAssignments {
			origIdx := clusterVecIndices[i]
			if subCluster == 0 {
				// Keep original assignment
				newAssignments[origIdx] = clusterID
			} else {
				// Assign to new cluster
				newClusterID := newNClusters + subCluster - 1
				newAssignments[origIdx] = newClusterID
			}
		}

		// Update centroid for original cluster
		copy(newCentroids[clusterID*dims:(clusterID+1)*dims], subCentroids[0:dims])

		// Add new centroids
		for i := 1; i < numSplits; i++ {
			newCentroids = append(newCentroids, subCentroids[i*dims:(i+1)*dims]...)
		}
		newNClusters += numSplits - 1
	}

	return newCentroids, newAssignments, newNClusters
}
