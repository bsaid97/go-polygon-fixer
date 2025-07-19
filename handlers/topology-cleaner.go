package handlers

import (
	"encoding/json"
	"fmt"
	"log"
	"runtime"

	"github.com/bsaid97/go-polygon-fixer/utils"
	"github.com/twpayne/go-geos"
)

type TopologyCleaningResult struct {
	Type     string    `json:"type"`
	Features []Feature `json:"features"`
}

type Feature struct {
	Type       string                 `json:"type"`
	Geometry   json.RawMessage        `json:"geometry"`
	Properties map[string]interface{} `json:"properties"`
}

func CleanTopology(geometryPayload string) (*TopologyCleaningResult, error) {
	// Add panic recovery to prevent server crashes
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC recovered in CleanTopology: %v", r)
		}
	}()
	
	log.Printf("=== CleanTopology function started ===")
	log.Printf("Payload length: %d characters", len(geometryPayload))
	var featureCollection struct {
		Type     string    `json:"type"`
		Features []Feature `json:"features"`
	}

	if err := json.Unmarshal([]byte(geometryPayload), &featureCollection); err != nil {
		return nil, fmt.Errorf("failed to parse feature collection: %v", err)
	}

	fmt.Printf("Processing %d features for topology cleaning\n", len(featureCollection.Features))

	// Calculate snap tolerance based on 40cm gaps in real-world data
	snapTolerance := utils.CalculateWGS84ToleranceFromMeters(0.4) // 40cm tolerance
	fmt.Printf("Using snap tolerance: %e degrees (40cm)\n", snapTolerance)

	// Create spatial index for efficient neighbor detection
	spatialIndex := utils.NewSpatialIndex(snapTolerance * 100) // Use larger cells for efficiency

	// Parse geometries in parallel
	geomFeatures, err := parseGeometriesParallel(featureCollection.Features)
	if err != nil {
		return nil, fmt.Errorf("failed to parse geometries: %v", err)
	}
	
	// Keep a copy of original geometries for boundary preservation validation
	originalGeomFeatures := make([]GeomFeature, len(geomFeatures))
	for i, geomFeature := range geomFeatures {
		if geomFeature.Geom != nil {
			clonedGeom := geomFeature.Geom.Clone()
			originalGeomFeatures[i] = GeomFeature{
				Geom:       clonedGeom,
				Properties: geomFeature.Properties,
			}
		}
	}

	// Build spatial index
	for i, geomFeature := range geomFeatures {
		spatialIndex.AddGeometry(geomFeature.Geom, i, geomFeature.Properties)
	}

	fmt.Printf("Successfully indexed %d polygon geometries\n", len(geomFeatures))

	// Clean topology by snapping nearby boundaries in parallel
	log.Printf("About to start boundary snapping...")
	cleanedGeometries, err := snapBoundariesParallel(geomFeatures, spatialIndex, snapTolerance)
	if err != nil {
		return nil, fmt.Errorf("failed to snap boundaries: %v", err)
	}

	// Validate and repair geometries in parallel
	log.Printf("About to start geometry validation and repair...")
	validatedGeometries, err := validateAndRepairGeometriesParallel(cleanedGeometries)
	if err != nil {
		return nil, fmt.Errorf("failed to validate geometries: %v", err)
	}

	// Perform coverage validation in parallel
	log.Printf("About to start coverage validation...")
	coverageReport := validateCoverageParallel(validatedGeometries, snapTolerance)
	log.Printf("Coverage validation finished: %d gaps (%d boundary segments), %d overlaps", 
		coverageReport.GapCount, coverageReport.BoundaryGaps, coverageReport.OverlapCount)
	log.Printf("Gap details: max width: %f, total length: %f", 
		coverageReport.MaxGapWidth, coverageReport.TotalGapLength)

	// Validate boundary preservation
	log.Printf("Validating boundary preservation...")
	preservationReport := validateBoundaryPreservation(originalGeomFeatures, validatedGeometries, snapTolerance)
	log.Printf("Boundary preservation: %d/%d geometries had significant changes (avg distortion: %f, max: %f)", 
		preservationReport.SignificantChanges, preservationReport.TotalGeometries, 
		preservationReport.AverageDistortion, preservationReport.MaxDistortion)

	// Clean up original geometry copies
	for _, geomFeature := range originalGeomFeatures {
		if geomFeature.Geom != nil {
			geomFeature.Geom.Destroy()
		}
	}

	// Convert back to GeoJSON feature collection
	result := &TopologyCleaningResult{
		Type:     "FeatureCollection",
		Features: make([]Feature, 0),
	}

	for _, geomFeature := range validatedGeometries {
		if geomFeature.Geom != nil {
			jsonString := geomFeature.Geom.ToGeoJSON(-1)
			feature := Feature{
				Type:       "Feature",
				Properties: geomFeature.Properties,
				Geometry:   json.RawMessage(jsonString),
			}
			result.Features = append(result.Features, feature)
		}
	}

	fmt.Printf("Topology cleaning complete. Processed %d features\n", len(result.Features))
	return result, nil
}

// CleanTopologyWithShapefile performs topology cleaning and returns both JSON and shapefile in a zip
func CleanTopologyWithShapefile(geometryPayload string) ([]byte, error) {
	// Add panic recovery to prevent server crashes
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC recovered in CleanTopologyWithShapefile: %v", r)
		}
	}()
	
	log.Printf("=== CleanTopologyWithShapefile function started ===")
	log.Printf("Payload length: %d characters", len(geometryPayload))
	// First get the cleaned topology result
	result, err := CleanTopology(geometryPayload)
	if err != nil {
		return nil, fmt.Errorf("topology cleaning failed: %v", err)
	}

	// Convert result to JSON
	jsonData, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal result to JSON: %v", err)
	}

	// Convert features to interface{} slice for shapefile generation
	features := make([]interface{}, len(result.Features))
	for i, feature := range result.Features {
		featureMap := map[string]interface{}{
			"type":       feature.Type,
			"geometry":   json.RawMessage(feature.Geometry),
			"properties": feature.Properties,
		}
		features[i] = featureMap
	}

	// Generate zip file with both JSON and shapefile
	zipData, err := utils.GenerateShapefileZip(jsonData, features)
	if err != nil {
		return nil, fmt.Errorf("failed to generate shapefile zip: %v", err)
	}

	return zipData, nil
}

type GeomFeature struct {
	Geom       *geos.Geom
	Properties map[string]interface{}
}

// ParsingJob represents a job for parallel geometry parsing
type ParsingJob struct {
	Feature Feature
	Index   int
}

// ParsingResult represents the result of parallel geometry parsing
type ParsingResult struct {
	GeomFeature GeomFeature
	Index       int
	Error       error
}

// SnappingJob represents a job for parallel boundary snapping
type SnappingJob struct {
	GeomFeature   GeomFeature
	Index         int
	SpatialIndex  *utils.SpatialIndex
	Tolerance     float64
}

// SnappingResult represents the result of parallel boundary snapping
type SnappingResult struct {
	GeomFeature GeomFeature
	Index       int
	WasSnapped  bool
	Error       error
}

// ValidationJob represents a job for parallel geometry validation
type ValidationJob struct {
	GeomFeature GeomFeature
	Index       int
}

// ValidationResult represents the result of parallel geometry validation
type ValidationResult struct {
	GeomFeature GeomFeature
	Index       int
	WasRepaired bool
	Error       error
}

// CoverageJob represents a job for parallel coverage validation
type CoverageJob struct {
	GeomI     *geos.Geom
	GeomJ     *geos.Geom
	IndexI    int
	IndexJ    int
	Tolerance float64
}

// CoverageResult represents the result of parallel coverage validation
type CoverageResult struct {
	IndexI         int
	IndexJ         int
	HasOverlap     bool
	OverlapArea    float64
	HasGap         bool
	GapDistance    float64
	MaxGapWidth    float64
	BoundaryGaps   int      // Number of boundary segments with gaps
	Error          error
}

// parseGeometriesParallel parses geometries in parallel using worker pool
func parseGeometriesParallel(features []Feature) ([]GeomFeature, error) {
	if len(features) == 0 {
		return []GeomFeature{}, nil
	}

	// Create parallel processor
	processor := utils.NewParallelProcessor(runtime.NumCPU())
	
	// Convert features to jobs
	jobs := make([]interface{}, len(features))
	for i, feature := range features {
		jobs[i] = ParsingJob{Feature: feature, Index: i}
	}
	
	// Define parsing work function
	parseGeometry := func(job interface{}) interface{} {
		parsingJob := job.(ParsingJob)
		
		// Marshal geometry to JSON
		jsonString, err := json.Marshal(parsingJob.Feature.Geometry)
		if err != nil {
			return ParsingResult{Error: fmt.Errorf("error marshalling geometry for feature %d: %v", parsingJob.Index, err)}
		}

		// Create GEOS geometry from JSON
		geom, err := geos.NewGeomFromGeoJSON(string(jsonString))
		if err != nil {
			return ParsingResult{Error: fmt.Errorf("error creating geometry for feature %d: %v", parsingJob.Index, err)}
		}

		// Only process valid polygon geometries
		if geom.TypeID() == 3 || geom.TypeID() == 6 { // Polygon or MultiPolygon
			// Check for empty or null geometries
			if geom.IsEmpty() {
				geom.Destroy()
				return ParsingResult{Error: fmt.Errorf("skipping empty geometry at feature %d", parsingJob.Index)}
			}
			
			geomFeature := GeomFeature{
				Geom:       geom,
				Properties: parsingJob.Feature.Properties,
			}
			
			return ParsingResult{
				GeomFeature: geomFeature,
				Index:       parsingJob.Index,
				Error:       nil,
			}
		} else {
			geom.Destroy()
			return ParsingResult{Error: fmt.Errorf("skipping non-polygon geometry at feature %d (type: %d)", parsingJob.Index, geom.TypeID())}
		}
	}
	
	// Process jobs in parallel
	results, err := processor.ProcessBatch(jobs, parseGeometry, "Parsing geometries")
	if err != nil {
		return nil, err
	}
	
	// Collect valid results
	validGeomFeatures := make([]GeomFeature, 0)
	invalidCount := 0
	
	for _, result := range results {
		if result != nil {
			parsingResult := result.(ParsingResult)
			if parsingResult.Error != nil {
				invalidCount++
				log.Printf("Parsing error: %v", parsingResult.Error)
			} else {
				validGeomFeatures = append(validGeomFeatures, parsingResult.GeomFeature)
			}
		}
	}
	
	if invalidCount > 0 {
		fmt.Printf("Skipped %d invalid geometries during parsing\n", invalidCount)
	}
	
	return validGeomFeatures, nil
}

// snapBoundariesParallel performs boundary snapping in parallel using worker pool
func snapBoundariesParallel(geomFeatures []GeomFeature, spatialIndex *utils.SpatialIndex, tolerance float64) ([]GeomFeature, error) {
	fmt.Printf("Starting parallel boundary snapping with tolerance: %e\n", tolerance)
	
	if len(geomFeatures) == 0 {
		return []GeomFeature{}, nil
	}

	// Create parallel processor
	processor := utils.NewParallelProcessor(runtime.NumCPU())
	
	// Convert geometry features to jobs
	jobs := make([]interface{}, len(geomFeatures))
	for i, geomFeature := range geomFeatures {
		jobs[i] = SnappingJob{
			GeomFeature:  geomFeature,
			Index:        i,
			SpatialIndex: spatialIndex,
			Tolerance:    tolerance,
		}
	}
	
	// Define snapping work function
	snapGeometry := func(job interface{}) interface{} {
		snappingJob := job.(SnappingJob)
		
		if snappingJob.GeomFeature.Geom == nil {
			return SnappingResult{
				GeomFeature: snappingJob.GeomFeature,
				Index:       snappingJob.Index,
				WasSnapped:  false,
				Error:       nil,
			}
		}
		
		// Find neighboring geometries
		neighbors := snappingJob.SpatialIndex.FindNeighbors(
			snappingJob.GeomFeature.Geom, 
			snappingJob.Tolerance*5, // Use larger search radius
		)
		
		if len(neighbors) == 0 {
			// No neighbors, keep original geometry
			return SnappingResult{
				GeomFeature: snappingJob.GeomFeature,
				Index:       snappingJob.Index,
				WasSnapped:  false,
				Error:       nil,
			}
		}
		
		snappedGeom := snappingJob.GeomFeature.Geom
		wasSnapped := false
		
		// Calculate maximum allowed distortion (proportional to tolerance)
		maxDistortion := snappingJob.Tolerance * 0.1 // Allow 10% of tolerance as distortion
		
		// Snap to each neighbor with conservative limits
		for _, neighbor := range neighbors {
			if neighbor.Geom != nil && neighbor.Index != snappingJob.Index {
				// Use conservative snapping
				tempSnapped, snapSuccessful := conservativeSnap(snappedGeom, neighbor.Geom, snappingJob.Tolerance, maxDistortion)
				if snapSuccessful && tempSnapped != snappedGeom {
					// Only destroy if it's not the original geometry
					if snappedGeom != snappingJob.GeomFeature.Geom {
						snappedGeom.Destroy()
					}
					snappedGeom = tempSnapped
					wasSnapped = true
				}
			}
		}
		
		return SnappingResult{
			GeomFeature: GeomFeature{
				Geom:       snappedGeom,
				Properties: snappingJob.GeomFeature.Properties,
			},
			Index:      snappingJob.Index,
			WasSnapped: wasSnapped,
			Error:      nil,
		}
	}
	
	// Process jobs in parallel
	results, err := processor.ProcessBatch(jobs, snapGeometry, "Snapping boundaries")
	if err != nil {
		return nil, err
	}
	
	// Collect results in order
	resultGeometries := make([]GeomFeature, len(geomFeatures))
	snappedCount := 0
	
	for _, result := range results {
		if result != nil {
			snappingResult := result.(SnappingResult)
			if snappingResult.Error != nil {
				log.Printf("Snapping error for geometry %d: %v", snappingResult.Index, snappingResult.Error)
				// Use original geometry if snapping failed
				resultGeometries[snappingResult.Index] = geomFeatures[snappingResult.Index]
			} else {
				resultGeometries[snappingResult.Index] = snappingResult.GeomFeature
				if snappingResult.WasSnapped {
					snappedCount++
				}
			}
		}
	}
	
	fmt.Printf("Parallel boundary snapping complete. Snapped %d of %d geometries\n", snappedCount, len(geomFeatures))
	return resultGeometries, nil
}

// validateAndRepairGeometriesParallel validates and repairs geometries in parallel
func validateAndRepairGeometriesParallel(geomFeatures []GeomFeature) ([]GeomFeature, error) {
	fmt.Printf("Starting parallel geometry validation and repair\n")
	
	if len(geomFeatures) == 0 {
		return []GeomFeature{}, nil
	}

	// Create parallel processor
	processor := utils.NewParallelProcessor(runtime.NumCPU())
	
	// Convert geometry features to jobs
	jobs := make([]interface{}, len(geomFeatures))
	for i, geomFeature := range geomFeatures {
		jobs[i] = ValidationJob{
			GeomFeature: geomFeature,
			Index:       i,
		}
	}
	
	// Define validation work function
	validateGeometry := func(job interface{}) interface{} {
		validationJob := job.(ValidationJob)
		
		if validationJob.GeomFeature.Geom == nil {
			return ValidationResult{
				GeomFeature: validationJob.GeomFeature,
				Index:       validationJob.Index,
				WasRepaired: false,
				Error:       fmt.Errorf("nil geometry at index %d", validationJob.Index),
			}
		}
		
		geom := validationJob.GeomFeature.Geom
		wasRepaired := false
		
		// Check if geometry is valid
		if !geom.IsValid() {
			fmt.Printf("Repairing invalid geometry at index %d: %s\n", validationJob.Index, geom.IsValidReason())
			
			// Make geometry valid
			repairedGeom := geom.MakeValidWithParams(geos.MakeValidLinework, geos.MakeValidDiscardCollapsed)
			if repairedGeom != nil {
				geom.Destroy()
				geom = repairedGeom
				wasRepaired = true
			}
		}
		
		// Apply coordinate truncation for precision consistency
		truncatedGeom, err := utils.TruncateFullGeometry(geom)
		if err != nil {
			log.Printf("Error truncating geometry at index %d: %v", validationJob.Index, err)
			return ValidationResult{
				GeomFeature: GeomFeature{
					Geom:       geom,
					Properties: validationJob.GeomFeature.Properties,
				},
				Index:       validationJob.Index,
				WasRepaired: wasRepaired,
				Error:       err,
			}
		}
		
		if truncatedGeom != geom {
			geom.Destroy()
		}
		
		return ValidationResult{
			GeomFeature: GeomFeature{
				Geom:       truncatedGeom,
				Properties: validationJob.GeomFeature.Properties,
			},
			Index:       validationJob.Index,
			WasRepaired: wasRepaired,
			Error:       nil,
		}
	}
	
	// Process jobs in parallel
	results, err := processor.ProcessBatch(jobs, validateGeometry, "Validating geometries")
	if err != nil {
		return nil, err
	}
	
	// Collect results in order
	resultGeometries := make([]GeomFeature, len(geomFeatures))
	repairedCount := 0
	errorCount := 0
	
	for _, result := range results {
		if result != nil {
			validationResult := result.(ValidationResult)
			resultGeometries[validationResult.Index] = validationResult.GeomFeature
			
			if validationResult.WasRepaired {
				repairedCount++
			}
			if validationResult.Error != nil {
				errorCount++
				log.Printf("Validation error for geometry %d: %v", validationResult.Index, validationResult.Error)
			}
		}
	}
	
	fmt.Printf("Parallel geometry validation complete. Repaired %d geometries, %d errors\n", repairedCount, errorCount)
	return resultGeometries, nil
}

// calculateGeometryDistortion measures how much a geometry has been distorted
func calculateGeometryDistortion(original, modified *geos.Geom) float64 {
	if original == nil || modified == nil {
		return 0.0
	}
	
	// Calculate area change ratio
	originalArea := original.Area()
	modifiedArea := modified.Area()
	
	if originalArea == 0 {
		return 0.0
	}
	
	areaChangeRatio := (modifiedArea - originalArea) / originalArea
	if areaChangeRatio < 0 {
		areaChangeRatio = -areaChangeRatio
	}
	
	// Calculate Hausdorff distance (measure of shape change)
	hausdorffDist := original.HausdorffDistance(modified)
	
	// Return combined distortion metric
	return areaChangeRatio + hausdorffDist
}

// conservativeSnap performs snapping with distortion limits
func conservativeSnap(geom, target *geos.Geom, tolerance float64, maxDistortion float64) (*geos.Geom, bool) {
	if geom == nil || target == nil {
		return geom, false
	}
	
	// Try snapping
	snappedGeom := geom.Snap(target, tolerance)
	if snappedGeom == nil {
		return geom, false
	}
	
	// Check distortion
	distortion := calculateGeometryDistortion(geom, snappedGeom)
	
	// If distortion is too high, reject the snap
	if distortion > maxDistortion {
		snappedGeom.Destroy()
		return geom, false
	}
	
	return snappedGeom, true
}

// BoundaryPreservationReport tracks how well boundaries are preserved
type BoundaryPreservationReport struct {
	TotalGeometries      int
	SignificantChanges   int
	AverageDistortion    float64
	MaxDistortion        float64
	GeometriesRejected   int
}

// validateBoundaryPreservation checks how well original boundaries are preserved
func validateBoundaryPreservation(originalGeoms, cleanedGeoms []GeomFeature, tolerance float64) BoundaryPreservationReport {
	report := BoundaryPreservationReport{
		TotalGeometries:    len(originalGeoms),
		SignificantChanges: 0,
		AverageDistortion:  0.0,
		MaxDistortion:      0.0,
		GeometriesRejected: 0,
	}
	
	if len(originalGeoms) != len(cleanedGeoms) {
		log.Printf("Warning: Geometry count mismatch in boundary preservation validation")
		return report
	}
	
	totalDistortion := 0.0
	significantChangeThreshold := tolerance * 0.5 // 50% of tolerance
	
	for i := 0; i < len(originalGeoms); i++ {
		if originalGeoms[i].Geom == nil || cleanedGeoms[i].Geom == nil {
			continue
		}
		
		distortion := calculateGeometryDistortion(originalGeoms[i].Geom, cleanedGeoms[i].Geom)
		totalDistortion += distortion
		
		if distortion > report.MaxDistortion {
			report.MaxDistortion = distortion
		}
		
		if distortion > significantChangeThreshold {
			report.SignificantChanges++
			log.Printf("Significant boundary change detected for geometry %d: distortion %f", i, distortion)
		}
	}
	
	if len(originalGeoms) > 0 {
		report.AverageDistortion = totalDistortion / float64(len(originalGeoms))
	}
	
	return report
}

// fillGapBetweenGeometries creates connecting geometry to fill gaps between adjacent polygons
func fillGapBetweenGeometries(geomI, geomJ *geos.Geom, tolerance float64) (*geos.Geom, error) {
	if geomI == nil || geomJ == nil {
		return nil, fmt.Errorf("cannot fill gap between nil geometries")
	}
	
	// Get boundaries
	boundaryI := geomI.Boundary()
	boundaryJ := geomJ.Boundary()
	
	if boundaryI == nil || boundaryJ == nil {
		if boundaryI != nil {
			boundaryI.Destroy()
		}
		if boundaryJ != nil {
			boundaryJ.Destroy()
		}
		return nil, fmt.Errorf("cannot extract boundaries")
	}
	
	defer boundaryI.Destroy()
	defer boundaryJ.Destroy()
	
	// Check if gap is worth filling
	distance := boundaryI.Distance(boundaryJ)
	if distance > tolerance*2 { // Only fill small gaps
		return nil, fmt.Errorf("gap too large to fill safely: %f", distance)
	}
	
	// Create buffer around boundaries to find connection area
	bufferI := boundaryI.Buffer(distance/2, 4) // Small buffer
	bufferJ := boundaryJ.Buffer(distance/2, 4)
	
	if bufferI == nil || bufferJ == nil {
		if bufferI != nil {
			bufferI.Destroy()
		}
		if bufferJ != nil {
			bufferJ.Destroy()
		}
		return nil, fmt.Errorf("failed to create buffers")
	}
	
	defer bufferI.Destroy()
	defer bufferJ.Destroy()
	
	// Find intersection of buffers (this is our gap area)
	gapArea := bufferI.Intersection(bufferJ)
	if gapArea == nil {
		return nil, fmt.Errorf("no gap area found")
	}
	
	// Only return small gap areas (prevent excessive modification)
	if gapArea.Area() > tolerance*tolerance*10 {
		gapArea.Destroy()
		return nil, fmt.Errorf("gap area too large: %f", gapArea.Area())
	}
	
	return gapArea, nil
}

// analyzeBoundaryGaps performs detailed boundary gap analysis between two geometries
func analyzeBoundaryGaps(geomI, geomJ *geos.Geom, tolerance float64) (bool, float64, float64, int) {
	// Get boundaries of both geometries
	boundaryI := geomI.Boundary()
	boundaryJ := geomJ.Boundary()
	
	if boundaryI == nil || boundaryJ == nil {
		if boundaryI != nil {
			boundaryI.Destroy()
		}
		if boundaryJ != nil {
			boundaryJ.Destroy()
		}
		return false, 0.0, 0.0, 0
	}
	
	defer boundaryI.Destroy()
	defer boundaryJ.Destroy()
	
	// Calculate distance between boundaries
	distance := boundaryI.Distance(boundaryJ)
	
	// Only consider potential gaps if geometries are close but not touching
	if distance <= tolerance || distance > tolerance*50 {
		return false, distance, 0.0, 0
	}
	
	// Check if boundaries are nearly parallel (indicating a potential gap)
	// Use buffering to find areas where boundaries are close
	bufferI := boundaryI.Buffer(tolerance*2, 8)
	bufferJ := boundaryJ.Buffer(tolerance*2, 8)
	
	if bufferI == nil || bufferJ == nil {
		if bufferI != nil {
			bufferI.Destroy()
		}
		if bufferJ != nil {
			bufferJ.Destroy()
		}
		return false, distance, 0.0, 0
	}
	
	defer bufferI.Destroy()
	defer bufferJ.Destroy()
	
	// Find intersection of buffered boundaries
	intersection := bufferI.Intersection(bufferJ)
	if intersection == nil {
		return false, distance, 0.0, 0
	}
	defer intersection.Destroy()
	
	// If there's significant intersection of buffers, we have a gap
	intersectionArea := intersection.Area()
	if intersectionArea > tolerance*tolerance {
		// Estimate gap characteristics
		maxGapWidth := distance
		boundaryGaps := 1
		
		// For linear features, estimate number of gap segments
		if intersection.Length() > tolerance*10 {
			boundaryGaps = int(intersection.Length() / (tolerance * 5))
			if boundaryGaps < 1 {
				boundaryGaps = 1
			}
		}
		
		return true, distance, maxGapWidth, boundaryGaps
	}
	
	return false, distance, 0.0, 0
}

// validateCoverageParallel performs coverage validation in parallel using worker pool
func validateCoverageParallel(geomFeatures []GeomFeature, tolerance float64) CoverageReport {
	log.Printf("=== Starting parallel coverage validation ===")
	log.Printf("Number of geometries to validate: %d", len(geomFeatures))
	log.Printf("Tolerance: %e degrees", tolerance)
	
	if len(geomFeatures) <= 1 {
		log.Printf("Not enough geometries for coverage validation")
		return CoverageReport{
			GapCount:        0,
			OverlapCount:    0,
			GapArea:         0.0,
			OverlapArea:     0.0,
			TotalGapLength:  0.0,
			MaxGapWidth:     0.0,
			BoundaryGaps:    0,
		}
	}

	// Create parallel processor
	processor := utils.NewParallelProcessor(runtime.NumCPU())
	
	// Generate all geometry pairs for comparison
	jobs := make([]interface{}, 0)
	for i := 0; i < len(geomFeatures); i++ {
		if geomFeatures[i].Geom == nil {
			continue
		}
		
		for j := i + 1; j < len(geomFeatures); j++ {
			if geomFeatures[j].Geom == nil {
				continue
			}
			
			jobs = append(jobs, CoverageJob{
				GeomI:     geomFeatures[i].Geom,
				GeomJ:     geomFeatures[j].Geom,
				IndexI:    i,
				IndexJ:    j,
				Tolerance: tolerance,
			})
		}
	}
	
	if len(jobs) == 0 {
		log.Printf("No geometry pairs to validate")
		return CoverageReport{
			GapCount:        0,
			OverlapCount:    0,
			GapArea:         0.0,
			OverlapArea:     0.0,
			TotalGapLength:  0.0,
			MaxGapWidth:     0.0,
			BoundaryGaps:    0,
		}
	}
	
	log.Printf("Generated %d geometry pairs for validation", len(jobs))
	
	// Define coverage validation work function
	validatePair := func(job interface{}) interface{} {
		coverageJob := job.(CoverageJob)
		
		result := CoverageResult{
			IndexI:         coverageJob.IndexI,
			IndexJ:         coverageJob.IndexJ,
			HasOverlap:     false,
			OverlapArea:    0.0,
			HasGap:         false,
			GapDistance:    0.0,
			MaxGapWidth:    0.0,
			BoundaryGaps:   0,
			Error:          nil,
		}
		
		// Debug: Show first few pairs being processed
		if coverageJob.IndexI < 3 && coverageJob.IndexJ < 5 {
			log.Printf("Processing pair (%d, %d)", coverageJob.IndexI, coverageJob.IndexJ)
		}
		
		// Calculate distance between geometries
		distance := coverageJob.GeomI.Distance(coverageJob.GeomJ)
		result.GapDistance = distance
		
		// Check for overlap first
		if coverageJob.GeomI.Overlaps(coverageJob.GeomJ) {
			intersection := coverageJob.GeomI.Intersection(coverageJob.GeomJ)
			if intersection != nil {
				area := intersection.Area()
				if area > coverageJob.Tolerance*coverageJob.Tolerance {
					result.HasOverlap = true
					result.OverlapArea = area
				}
				intersection.Destroy()
			}
		}
		
		// Perform detailed boundary gap analysis for nearby geometries
		if distance <= coverageJob.Tolerance*50 { // Only analyze reasonably close geometries
			hasGap, gapDistance, maxGapWidth, boundaryGaps := analyzeBoundaryGaps(
				coverageJob.GeomI, coverageJob.GeomJ, coverageJob.Tolerance)
			
			if hasGap {
				result.HasGap = true
				result.GapDistance = gapDistance
				result.MaxGapWidth = maxGapWidth
				result.BoundaryGaps = boundaryGaps
			}
		}
		
		return result
	}
	
	// Process jobs in parallel
	results, err := processor.ProcessBatch(jobs, validatePair, "Coverage validation")
	if err != nil {
		log.Printf("Error during parallel coverage validation: %v", err)
		return CoverageReport{
			GapCount:        0,
			OverlapCount:    0,
			GapArea:         0.0,
			OverlapArea:     0.0,
			TotalGapLength:  0.0,
			MaxGapWidth:     0.0,
			BoundaryGaps:    0,
		}
	}
	
	// Aggregate results
	report := CoverageReport{
		GapCount:        0,
		OverlapCount:    0,
		GapArea:         0.0,
		OverlapArea:     0.0,
		TotalGapLength:  0.0,
		MaxGapWidth:     0.0,
		BoundaryGaps:    0,
	}
	
	for _, result := range results {
		if result != nil {
			coverageResult := result.(CoverageResult)
			if coverageResult.Error != nil {
				log.Printf("Coverage validation error for pair (%d, %d): %v", 
					coverageResult.IndexI, coverageResult.IndexJ, coverageResult.Error)
				continue
			}
			
			if coverageResult.HasOverlap {
				report.OverlapCount++
				report.OverlapArea += coverageResult.OverlapArea
				
				log.Printf("*** OVERLAP DETECTED *** between features %d and %d (area: %f)", 
					coverageResult.IndexI, coverageResult.IndexJ, coverageResult.OverlapArea)
			}
			
			if coverageResult.HasGap {
				report.GapCount++
				report.BoundaryGaps += coverageResult.BoundaryGaps
				report.TotalGapLength += coverageResult.GapDistance
				
				if coverageResult.MaxGapWidth > report.MaxGapWidth {
					report.MaxGapWidth = coverageResult.MaxGapWidth
				}
				
				log.Printf("*** GAP DETECTED *** between features %d and %d (distance: %f, width: %f, segments: %d)", 
					coverageResult.IndexI, coverageResult.IndexJ, 
					coverageResult.GapDistance, coverageResult.MaxGapWidth, coverageResult.BoundaryGaps)
			}
		}
	}
	
	log.Printf("=== Parallel coverage validation complete ===")
	log.Printf("Results: %d gaps (%d boundary segments, max width: %f, total length: %f), %d overlaps (total area: %f)", 
		report.GapCount, report.BoundaryGaps, report.MaxGapWidth, report.TotalGapLength, 
		report.OverlapCount, report.OverlapArea)
	return report
}

func snapBoundaries(geomFeatures []GeomFeature, spatialIndex *utils.SpatialIndex, tolerance float64) ([]GeomFeature, error) {
	fmt.Printf("Starting boundary snapping with tolerance: %e\n", tolerance)
	
	result := make([]GeomFeature, len(geomFeatures))
	snappedCount := 0
	
	for i, geomFeature := range geomFeatures {
		if geomFeature.Geom == nil {
			continue
		}
		
		// Progress tracking
		if i%100 == 0 {
			fmt.Printf("Processing geometry %d of %d (%.1f%%)\n", i, len(geomFeatures), float64(i)/float64(len(geomFeatures))*100)
		}
		
		// Find neighboring geometries
		neighbors := spatialIndex.FindNeighbors(geomFeature.Geom, tolerance*5) // Use larger search radius
		
		if len(neighbors) == 0 {
			// No neighbors, keep original geometry
			result[i] = geomFeature
			continue
		}
		
		snappedGeom := geomFeature.Geom
		wasSnapped := false
		
		// Calculate maximum allowed distortion
		maxDistortion := tolerance * 0.1 // Allow 10% of tolerance as distortion
		
		// Snap to each neighbor with conservative limits
		for _, neighbor := range neighbors {
			if neighbor.Geom != nil && neighbor.Index != i {
				// Use conservative snapping
				tempSnapped, snapSuccessful := conservativeSnap(snappedGeom, neighbor.Geom, tolerance, maxDistortion)
				if snapSuccessful && tempSnapped != snappedGeom {
					if snappedGeom != geomFeature.Geom {
						snappedGeom.Destroy()
					}
					snappedGeom = tempSnapped
					wasSnapped = true
				}
			}
		}
		
		if wasSnapped {
			snappedCount++
		}
		
		result[i] = GeomFeature{
			Geom:       snappedGeom,
			Properties: geomFeature.Properties,
		}
	}
	
	fmt.Printf("Boundary snapping complete. Snapped %d of %d geometries\n", snappedCount, len(geomFeatures))
	return result, nil
}

func validateAndRepairGeometries(geomFeatures []GeomFeature) ([]GeomFeature, error) {
	fmt.Printf("Starting geometry validation and repair\n")
	
	result := make([]GeomFeature, len(geomFeatures))
	
	for i, geomFeature := range geomFeatures {
		if geomFeature.Geom == nil {
			continue
		}
		
		geom := geomFeature.Geom
		
		// Check if geometry is valid
		if !geom.IsValid() {
			fmt.Printf("Repairing invalid geometry at index %d: %s\n", i, geom.IsValidReason())
			
			// Make geometry valid
			repairedGeom := geom.MakeValidWithParams(geos.MakeValidLinework, geos.MakeValidDiscardCollapsed)
			if repairedGeom != nil {
				geom.Destroy()
				geom = repairedGeom
			}
		}
		
		// Apply coordinate truncation for precision consistency
		truncatedGeom, err := utils.TruncateFullGeometry(geom)
		if err != nil {
			log.Printf("Error truncating geometry at index %d: %v", i, err)
			result[i] = GeomFeature{
				Geom:       geom,
				Properties: geomFeature.Properties,
			}
		} else {
			if truncatedGeom != geom {
				geom.Destroy()
			}
			result[i] = GeomFeature{
				Geom:       truncatedGeom,
				Properties: geomFeature.Properties,
			}
		}
	}
	
	fmt.Printf("Geometry validation and repair complete\n")
	return result, nil
}

type CoverageReport struct {
	GapCount        int
	OverlapCount    int
	GapArea         float64
	OverlapArea     float64
	TotalGapLength  float64  // Total length of boundary gaps
	MaxGapWidth     float64  // Maximum gap width detected
	BoundaryGaps    int      // Total number of boundary gap segments
}

func validateCoverage(geomFeatures []GeomFeature, tolerance float64) CoverageReport {
	fmt.Printf("Starting coverage validation\n")
	
	report := CoverageReport{
		GapCount:        0,
		OverlapCount:    0,
		GapArea:         0.0,
		OverlapArea:     0.0,
		TotalGapLength:  0.0,
		MaxGapWidth:     0.0,
		BoundaryGaps:    0,
	}
	
	// Check for overlaps between adjacent polygons
	for i := 0; i < len(geomFeatures); i++ {
		if geomFeatures[i].Geom == nil {
			continue
		}
		
		for j := i + 1; j < len(geomFeatures); j++ {
			if geomFeatures[j].Geom == nil {
				continue
			}
			
			// Check if geometries are touching (adjacent)
			if geomFeatures[i].Geom.Touches(geomFeatures[j].Geom) || 
			   geomFeatures[i].Geom.Distance(geomFeatures[j].Geom) <= tolerance {
				
				// Check for overlap
				if geomFeatures[i].Geom.Overlaps(geomFeatures[j].Geom) {
					intersection := geomFeatures[i].Geom.Intersection(geomFeatures[j].Geom)
					if intersection != nil && intersection.Area() > tolerance*tolerance {
						report.OverlapCount++
						report.OverlapArea += intersection.Area()
						
						// Log overlap details
						fmt.Printf("Overlap detected between features %d and %d (area: %f)\n", 
							i, j, intersection.Area())
					}
					if intersection != nil {
						intersection.Destroy()
					}
				}
			}
		}
	}
	
	// Simple gap detection - check if boundaries are within tolerance
	gapCount := 0
	for i := 0; i < len(geomFeatures); i++ {
		if geomFeatures[i].Geom == nil {
			continue
		}
		
		boundary := geomFeatures[i].Geom.Boundary()
		if boundary == nil {
			continue
		}
		
		// Check if each boundary segment has a corresponding segment in adjacent polygons
		for j := 0; j < len(geomFeatures); j++ {
			if i == j || geomFeatures[j].Geom == nil {
				continue
			}
			
			distance := boundary.Distance(geomFeatures[j].Geom)
			if distance > tolerance && distance < tolerance*10 { // Potential gap
				gapCount++
				break
			}
		}
		
		boundary.Destroy()
	}
	
	report.GapCount = gapCount
	
	fmt.Printf("Coverage validation complete\n")
	return report
}