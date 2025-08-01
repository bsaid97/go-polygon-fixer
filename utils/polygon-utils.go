package utils

import (
	"fmt"
	"math"

	"github.com/twpayne/go-geos"
)

type FeatureCollection struct {
	Features []Features
	Type     string
}

type Coord struct {
	X float64
	Y float64
}

type Features struct {
	Type     string
	Geometry any
}


var PRECISION int = 7

// func createGoRoutine(polygon *geos.Geom) (*geos.Geom, error) {

// }

func TruncateFullGeometry(feature *geos.Geom) (*geos.Geom, error) {
	if feature == nil {
		return nil, fmt.Errorf(`geometry is nil`)
	}

	// Count total number of polygons to process
	totalPolygons := 0
	for i := range feature.NumGeometries() {
		geometry := feature.Geometry(i)
		if geometry.IsValid() {
			if geometry.TypeID() == 3 {
				totalPolygons++
			} else if geometry.TypeID() == 6 {
				for j := range geometry.NumGeometries() {
					singlePolygon := geometry.Geometry(j)
					if singlePolygon.TypeID() == 3 {
						totalPolygons++
					}
				}
			}
		}
	}

	if totalPolygons == 0 {
		return nil, fmt.Errorf("no valid polygons found")
	}

	// Create channel with correct buffer size
	polygons := make(chan *geos.Geom, totalPolygons)
	
	// Launch goroutines to process polygons
	for i := range feature.NumGeometries() {
		geometry := feature.Geometry(i)
		if geometry.IsValid() {
			if geometry.TypeID() == 3 {
				go func(polygon *geos.Geom) {
					polygons <- TruncateSinglePolygon(polygon)
				}(geometry)
			} else if geometry.TypeID() == 6 {
				for j := range geometry.NumGeometries() {
					singlePolygon := geometry.Geometry(j)
					if singlePolygon.TypeID() == 3 {
						go func(polygon *geos.Geom) {
							polygons <- TruncateSinglePolygon(polygon)
						}(singlePolygon)
					}
				}
			}
		}
	}

	// Collect results from all goroutines
	var newPolygons = make([]*geos.Geom, 0, totalPolygons)
	for i := 0; i < totalPolygons; i++ {
		result := <-polygons
		if result != nil {
			newPolygons = append(newPolygons, result)
		}
	}

	if len(newPolygons) == 0 {
		return nil, fmt.Errorf("no valid truncated polygons produced")
	}

	if len(newPolygons) == 1 {
		return newPolygons[0], nil
	}

	return geos.NewCollection(geos.TypeIDMultiPolygon, newPolygons), nil
}

func TruncateSinglePolygon(polygon *geos.Geom) *geos.Geom {
	var rings [][][]float64
	var outerRing [][]float64
	if polygon.ExteriorRing() != nil && polygon.ExteriorRing().CoordSeq().Size() > 3 {
		// fmt.Println("in ext ring")
		for j := range polygon.ExteriorRing().CoordSeq().Size() {
			x := polygon.ExteriorRing().CoordSeq().X(j)
			y := polygon.ExteriorRing().CoordSeq().Y(j)

			newX, newY := truncateCoordinates(x, y)
			outerRing = append(outerRing, []float64{newX, newY})
		}
		rings = append(rings, outerRing)
		outerRing = nil
		// fmt.Println("interior", polygon.NumInteriorRings())
		if polygon.NumInteriorRings() > 0 {
			for r := range polygon.NumInteriorRings() {
				var ringCoords [][]float64
				ring := polygon.InteriorRing(r)
				if ring.CoordSeq().Size() > 3 {
					for k := range ring.CoordSeq().Size() {
						x := ring.CoordSeq().X(k)
						y := ring.CoordSeq().Y(k)

						newX, newY := truncateCoordinates(x, y)
						ringCoords = append(ringCoords, []float64{newX, newY})
					}
					testPolygon := geos.NewPolygon([][][]float64{ringCoords})
					if len(ringCoords) > 0 && testPolygon.IsValid() {
						rings = append(rings, ringCoords)
					}
					ringCoords = nil
					testPolygon.Destroy()
				}
			}
		}

		return geos.NewPolygon(rings)
	}

	return nil
}

func truncateCoordinates(x float64, y float64) (float64, float64) {
	return roundFloat(x, uint(PRECISION)), roundFloat(y, uint(PRECISION))
}

func roundFloat(val float64, precision uint) float64 {
	ratio := math.Pow(10, float64(precision))
	return math.Round(val*ratio) / ratio
}
