package handlers

import (
	"encoding/json"
	"fmt"
	"log"

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
	var featureCollection struct {
		Type     string    `json:"type"`
		Features []Feature `json:"features"`
	}

	if err := json.Unmarshal([]byte(geometryPayload), &featureCollection); err != nil {
		return nil, fmt.Errorf("failed to parse feature collection: %v", err)
	}

	fmt.Printf("Processing %d features for topology cleaning\n", len(featureCollection.Features))

	// Calculate snap tolerance based on WGS84 precision
	snapTolerance := utils.CalculateWGS84Tolerance(7) // 7 decimal places
	fmt.Printf("Using snap tolerance: %e degrees\n", snapTolerance)

	// Create spatial index for efficient neighbor detection
	spatialIndex := utils.NewSpatialIndex(snapTolerance * 100) // Use larger cells for efficiency

	// Parse geometries and build spatial index
	geomFeatures := make([]GeomFeature, 0)
	invalidGeometries := 0
	
	for i, feature := range featureCollection.Features {
		jsonString, err := json.Marshal(feature.Geometry)
		if err != nil {
			log.Printf("Error marshalling geometry for feature %d: %v", i, err)
			continue
		}

		geom, err := geos.NewGeomFromGeoJSON(string(jsonString))
		if err != nil {
			log.Printf("Error creating geometry for feature %d: %v", i, err)
			continue
		}

		// Only process valid polygon geometries
		if geom.TypeID() == 3 || geom.TypeID() == 6 { // Polygon or MultiPolygon
			// Check for empty or null geometries
			if geom.IsEmpty() {
				log.Printf("Skipping empty geometry at feature %d", i)
				geom.Destroy()
				continue
			}
			
			geomFeature := GeomFeature{
				Geom:       geom,
				Properties: feature.Properties,
			}
			geomFeatures = append(geomFeatures, geomFeature)
			spatialIndex.AddGeometry(geom, len(geomFeatures)-1, feature.Properties)
		} else {
			log.Printf("Skipping non-polygon geometry at feature %d (type: %d)", i, geom.TypeID())
			invalidGeometries++
			geom.Destroy()
		}
	}
	
	if invalidGeometries > 0 {
		fmt.Printf("Skipped %d non-polygon geometries\n", invalidGeometries)
	}

	fmt.Printf("Successfully indexed %d polygon geometries\n", len(geomFeatures))

	// Clean topology by snapping nearby boundaries
	cleanedGeometries, err := snapBoundaries(geomFeatures, spatialIndex, snapTolerance)
	if err != nil {
		return nil, fmt.Errorf("failed to snap boundaries: %v", err)
	}

	// Validate and repair geometries
	validatedGeometries, err := validateAndRepairGeometries(cleanedGeometries)
	if err != nil {
		return nil, fmt.Errorf("failed to validate geometries: %v", err)
	}

	// Perform coverage validation
	coverageReport := validateCoverage(validatedGeometries, snapTolerance)
	fmt.Printf("Coverage validation: %d gaps detected, %d overlaps detected\n", 
		coverageReport.GapCount, coverageReport.OverlapCount)

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

type GeomFeature struct {
	Geom       *geos.Geom
	Properties map[string]interface{}
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
		
		// Snap to each neighbor
		for _, neighbor := range neighbors {
			if neighbor.Geom != nil && neighbor.Index != i {
				// Snap current geometry to neighbor
				tempSnapped := snappedGeom.Snap(neighbor.Geom, tolerance)
				if tempSnapped != nil {
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
	GapCount     int
	OverlapCount int
	GapArea      float64
	OverlapArea  float64
}

func validateCoverage(geomFeatures []GeomFeature, tolerance float64) CoverageReport {
	fmt.Printf("Starting coverage validation\n")
	
	report := CoverageReport{
		GapCount:     0,
		OverlapCount: 0,
		GapArea:      0.0,
		OverlapArea:  0.0,
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