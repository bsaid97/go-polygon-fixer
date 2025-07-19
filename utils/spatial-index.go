package utils

import (
	"fmt"
	"math"

	"github.com/twpayne/go-geos"
)

type SpatialIndex struct {
	geometries []*IndexedGeometry
	cellSize   float64
	grid       map[string][]*IndexedGeometry
}

type IndexedGeometry struct {
	Geom       *geos.Geom
	Index      int
	Properties map[string]interface{}
}

func NewSpatialIndex(cellSize float64) *SpatialIndex {
	return &SpatialIndex{
		geometries: make([]*IndexedGeometry, 0),
		cellSize:   cellSize,
		grid:       make(map[string][]*IndexedGeometry),
	}
}

func (si *SpatialIndex) AddGeometry(geom *geos.Geom, index int, properties map[string]interface{}) {
	if geom == nil {
		fmt.Printf("Warning: nil geometry passed to AddGeometry at index %d\n", index)
		return
	}

	indexedGeom := &IndexedGeometry{
		Geom:       geom,
		Index:      index,
		Properties: properties,
	}

	si.geometries = append(si.geometries, indexedGeom)
	si.addToGrid(indexedGeom)
}

func (si *SpatialIndex) addToGrid(indexedGeom *IndexedGeometry) {
	geom := indexedGeom.Geom

	// Check if geometry is valid
	if geom == nil {
		fmt.Printf("Warning: nil geometry for index %d\n", indexedGeom.Index)
		return
	}

	// Get bounds directly from geometry using Bounds() method
	bounds := geom.Bounds()
	if bounds == nil {
		fmt.Printf("Warning: nil bounds for geometry at index %d\n", indexedGeom.Index)
		return
	}

	// Access bounds directly from Box2D struct
	minX := bounds.MinX
	minY := bounds.MinY
	maxX := bounds.MaxX
	maxY := bounds.MaxY

	minCellX := int(math.Floor(minX / si.cellSize))
	minCellY := int(math.Floor(minY / si.cellSize))
	maxCellX := int(math.Floor(maxX / si.cellSize))
	maxCellY := int(math.Floor(maxY / si.cellSize))

	for x := minCellX; x <= maxCellX; x++ {
		for y := minCellY; y <= maxCellY; y++ {
			cellKey := getCellKey(x, y)
			si.grid[cellKey] = append(si.grid[cellKey], indexedGeom)
		}
	}
}

func (si *SpatialIndex) FindNeighbors(geom *geos.Geom, distance float64) []*IndexedGeometry {
	buffer := geom.Buffer(distance, 8)
	if buffer == nil {
		fmt.Printf("Warning: failed to create buffer in FindNeighbors\n")
		return []*IndexedGeometry{}
	}

	// Get bounds directly from buffered geometry
	bounds := buffer.Bounds()
	if bounds == nil {
		fmt.Printf("Warning: nil bounds for buffered geometry in FindNeighbors\n")
		buffer.Destroy()
		return []*IndexedGeometry{}
	}

	// Access bounds directly from Box2D struct
	minX := bounds.MinX
	minY := bounds.MinY
	maxX := bounds.MaxX
	maxY := bounds.MaxY

	minCellX := int(math.Floor(minX / si.cellSize))
	minCellY := int(math.Floor(minY / si.cellSize))
	maxCellX := int(math.Floor(maxX / si.cellSize))
	maxCellY := int(math.Floor(maxY / si.cellSize))

	candidates := make(map[int]*IndexedGeometry)

	for x := minCellX; x <= maxCellX; x++ {
		for y := minCellY; y <= maxCellY; y++ {
			cellKey := getCellKey(x, y)
			if cell, exists := si.grid[cellKey]; exists {
				for _, candidate := range cell {
					if candidate.Geom != geom {
						candidates[candidate.Index] = candidate
					}
				}
			}
		}
	}

	neighbors := make([]*IndexedGeometry, 0)
	for _, candidate := range candidates {
		if geom.Distance(candidate.Geom) <= distance {
			neighbors = append(neighbors, candidate)
		}
	}

	buffer.Destroy()

	return neighbors
}

func getCellKey(x, y int) string {
	return fmt.Sprintf("%d,%d", x, y)
}

func CalculateWGS84Tolerance(precisionDecimals int) float64 {
	baseTolerance := math.Pow(10, float64(-precisionDecimals))
	return baseTolerance * 10
}

// CalculateWGS84ToleranceFromMeters converts meters to WGS84 degrees
// For WGS84, 1 degree ≈ 111,000 meters at the equator
func CalculateWGS84ToleranceFromMeters(meters float64) float64 {
	const metersPerDegree = 111000.0
	return meters / metersPerDegree
}
