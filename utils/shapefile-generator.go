package utils

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/jonas-p/go-shp"
)

// GeometryFromGeoJSON represents a simplified geometry structure for conversion
type GeometryFromGeoJSON struct {
	Type        string          `json:"type"`
	Coordinates json.RawMessage `json:"coordinates"`
}

// GenerateShapefileZip creates a zip file containing both JSON and shapefile formats
func GenerateShapefileZip(jsonData []byte, features []interface{}) ([]byte, error) {
	// Create a buffer to write the zip file
	var zipBuffer bytes.Buffer
	zipWriter := zip.NewWriter(&zipBuffer)

	// Add JSON file to zip
	jsonFile, err := zipWriter.Create("cleaned_topology.json")
	if err != nil {
		return nil, fmt.Errorf("failed to create JSON file in zip: %v", err)
	}
	_, err = jsonFile.Write(jsonData)
	if err != nil {
		return nil, fmt.Errorf("failed to write JSON data to zip: %v", err)
	}

	// Generate shapefile and add to zip
	err = addShapefileToZip(zipWriter, features)
	if err != nil {
		return nil, fmt.Errorf("failed to add shapefile to zip: %v", err)
	}

	// Close the zip writer
	err = zipWriter.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to close zip writer: %v", err)
	}

	return zipBuffer.Bytes(), nil
}

// addShapefileToZip creates shapefile components and adds them to the zip
func addShapefileToZip(zipWriter *zip.Writer, features []interface{}) error {
	// Create temporary directory for shapefile generation
	tempDir, err := os.MkdirTemp("", "shapefile_")
	if err != nil {
		return fmt.Errorf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create shapefile path
	shapefilePath := filepath.Join(tempDir, "cleaned_topology.shp")

	// Generate shapefile
	err = generateShapefile(shapefilePath, features)
	if err != nil {
		return fmt.Errorf("failed to generate shapefile: %v", err)
	}

	// Add shapefile components to zip
	extensions := []string{".shp", ".shx", ".dbf"}
	for _, ext := range extensions {
		filePath := strings.TrimSuffix(shapefilePath, ".shp") + ext
		
		// Check if file exists
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			continue
		}

		// Read file content
		fileContent, err := os.ReadFile(filePath)
		if err != nil {
			return fmt.Errorf("failed to read shapefile component %s: %v", ext, err)
		}

		// Add to zip
		zipFile, err := zipWriter.Create("cleaned_topology" + ext)
		if err != nil {
			return fmt.Errorf("failed to create %s file in zip: %v", ext, err)
		}

		_, err = zipFile.Write(fileContent)
		if err != nil {
			return fmt.Errorf("failed to write %s data to zip: %v", ext, err)
		}
	}

	return nil
}

// generateShapefile creates a shapefile from the feature collection
func generateShapefile(shapefilePath string, features []interface{}) error {
	if len(features) == 0 {
		return fmt.Errorf("no features to write to shapefile")
	}

	// Determine geometry type from first feature
	firstFeature, ok := features[0].(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid feature format")
	}

	geometryRaw, ok := firstFeature["geometry"]
	if !ok {
		return fmt.Errorf("feature missing geometry")
	}

	// Parse geometry to determine type
	var geom GeometryFromGeoJSON
	geometryBytes, err := json.Marshal(geometryRaw)
	if err != nil {
		return fmt.Errorf("failed to marshal geometry: %v", err)
	}

	err = json.Unmarshal(geometryBytes, &geom)
	if err != nil {
		return fmt.Errorf("failed to unmarshal geometry: %v", err)
	}

	// Map GeoJSON geometry type to shapefile type
	var shapeType shp.ShapeType
	switch geom.Type {
	case "Point":
		shapeType = shp.POINT
	case "LineString", "MultiLineString":
		shapeType = shp.POLYLINE
	case "Polygon", "MultiPolygon":
		shapeType = shp.POLYGON
	default:
		return fmt.Errorf("unsupported geometry type: %s", geom.Type)
	}

	// Create shapefile
	shape, err := shp.Create(shapefilePath, shapeType)
	if err != nil {
		return fmt.Errorf("failed to create shapefile: %v", err)
	}
	defer shape.Close()

	// Determine fields from properties of first feature
	properties, ok := firstFeature["properties"].(map[string]interface{})
	if !ok {
		properties = make(map[string]interface{})
	}

	fields := createFieldsFromProperties(properties)
	shape.SetFields(fields)

	// Write features to shapefile
	for i, featureRaw := range features {
		feature, ok := featureRaw.(map[string]interface{})
		if !ok {
			continue
		}

		// Parse geometry
		geometryRaw, ok := feature["geometry"]
		if !ok {
			continue
		}

		geometryBytes, err := json.Marshal(geometryRaw)
		if err != nil {
			fmt.Printf("Warning: failed to marshal geometry for feature %d: %v\n", i, err)
			continue
		}

		var geom GeometryFromGeoJSON
		err = json.Unmarshal(geometryBytes, &geom)
		if err != nil {
			fmt.Printf("Warning: failed to unmarshal geometry for feature %d: %v\n", i, err)
			continue
		}

		// Convert geometry to shapefile format and write
		err = writeGeometryToShapefile(shape, &geom, shapeType)
		if err != nil {
			fmt.Printf("Warning: failed to write geometry for feature %d: %v\n", i, err)
			continue
		}

		// Write attributes
		properties, ok := feature["properties"].(map[string]interface{})
		if !ok {
			properties = make(map[string]interface{})
		}

		err = writeAttributesToShapefile(shape, properties, fields, i)
		if err != nil {
			fmt.Printf("Warning: failed to write attributes for feature %d: %v\n", i, err)
		}
	}

	return nil
}

// createFieldsFromProperties analyzes properties to create DBF fields
func createFieldsFromProperties(properties map[string]interface{}) []shp.Field {
	fields := []shp.Field{}

	for key, value := range properties {
		// Limit field name to 10 characters (DBF limitation)
		fieldName := key
		if len(fieldName) > 10 {
			fieldName = fieldName[:10]
		}

		switch v := value.(type) {
		case string:
			// Determine appropriate length, max 254 for DBF
			length := len(v)
			if length < 50 {
				length = 50 // Default minimum
			}
			if length > 254 {
				length = 254
			}
			fields = append(fields, shp.StringField(fieldName, uint8(length)))
		case float64:
			fields = append(fields, shp.FloatField(fieldName, 15, 5))
		case int, int32, int64:
			fields = append(fields, shp.NumberField(fieldName, 15))
		case bool:
			fields = append(fields, shp.StringField(fieldName, 5)) // Store as "true"/"false"
		default:
			// Default to string field for unknown types
			fields = append(fields, shp.StringField(fieldName, 100))
		}
	}

	// Add a default ID field if no fields exist
	if len(fields) == 0 {
		fields = append(fields, shp.NumberField("ID", 10))
	}

	return fields
}

// writeGeometryToShapefile converts GeoJSON geometry to shapefile format and writes it
func writeGeometryToShapefile(shape *shp.Writer, geom *GeometryFromGeoJSON, shapeType shp.ShapeType) error {
	switch geom.Type {
	case "Point":
		return writePointGeometry(shape, geom)
	case "Polygon":
		return writePolygonGeometry(shape, geom)
	case "MultiPolygon":
		return writeMultiPolygonGeometry(shape, geom)
	case "LineString":
		return writeLineStringGeometry(shape, geom)
	case "MultiLineString":
		return writeMultiLineStringGeometry(shape, geom)
	default:
		return fmt.Errorf("unsupported geometry type: %s", geom.Type)
	}
}

// writePointGeometry writes a point geometry to shapefile
func writePointGeometry(shape *shp.Writer, geom *GeometryFromGeoJSON) error {
	var coords []float64
	err := json.Unmarshal(geom.Coordinates, &coords)
	if err != nil {
		return fmt.Errorf("failed to unmarshal point coordinates: %v", err)
	}

	if len(coords) < 2 {
		return fmt.Errorf("invalid point coordinates")
	}

	point := shp.Point{X: coords[0], Y: coords[1]}
	shape.Write(&point)
	return nil
}

// writePolygonGeometry writes a polygon geometry to shapefile
func writePolygonGeometry(shape *shp.Writer, geom *GeometryFromGeoJSON) error {
	var coords [][][]float64
	err := json.Unmarshal(geom.Coordinates, &coords)
	if err != nil {
		return fmt.Errorf("failed to unmarshal polygon coordinates: %v", err)
	}

	polygon := &shp.Polygon{}

	for _, ring := range coords {
		var points []shp.Point
		for _, coord := range ring {
			if len(coord) >= 2 {
				points = append(points, shp.Point{X: coord[0], Y: coord[1]})
			}
		}
		if len(points) > 0 {
			polygon.Points = append(polygon.Points, points...)
			polygon.Parts = append(polygon.Parts, int32(len(polygon.Points)-len(points)))
		}
	}

	// Fix parts indexing
	if len(polygon.Parts) > 0 {
		polygon.Parts[0] = 0
		for i := 1; i < len(polygon.Parts); i++ {
			polygon.Parts[i] = polygon.Parts[i-1] + int32(len(coords[i-1]))
		}
	}

	shape.Write(polygon)
	return nil
}

// writeMultiPolygonGeometry writes a multipolygon geometry to shapefile
func writeMultiPolygonGeometry(shape *shp.Writer, geom *GeometryFromGeoJSON) error {
	var coords [][][][]float64
	err := json.Unmarshal(geom.Coordinates, &coords)
	if err != nil {
		return fmt.Errorf("failed to unmarshal multipolygon coordinates: %v", err)
	}

	polygon := &shp.Polygon{}
	partIndex := int32(0)

	for _, poly := range coords {
		for _, ring := range poly {
			var points []shp.Point
			for _, coord := range ring {
				if len(coord) >= 2 {
					points = append(points, shp.Point{X: coord[0], Y: coord[1]})
				}
			}
			if len(points) > 0 {
				polygon.Parts = append(polygon.Parts, partIndex)
				polygon.Points = append(polygon.Points, points...)
				partIndex += int32(len(points))
			}
		}
	}

	shape.Write(polygon)
	return nil
}

// writeLineStringGeometry writes a linestring geometry to shapefile
func writeLineStringGeometry(shape *shp.Writer, geom *GeometryFromGeoJSON) error {
	var coords [][]float64
	err := json.Unmarshal(geom.Coordinates, &coords)
	if err != nil {
		return fmt.Errorf("failed to unmarshal linestring coordinates: %v", err)
	}

	polyline := &shp.PolyLine{}
	polyline.Parts = []int32{0}

	for _, coord := range coords {
		if len(coord) >= 2 {
			polyline.Points = append(polyline.Points, shp.Point{X: coord[0], Y: coord[1]})
		}
	}

	shape.Write(polyline)
	return nil
}

// writeMultiLineStringGeometry writes a multilinestring geometry to shapefile
func writeMultiLineStringGeometry(shape *shp.Writer, geom *GeometryFromGeoJSON) error {
	var coords [][][]float64
	err := json.Unmarshal(geom.Coordinates, &coords)
	if err != nil {
		return fmt.Errorf("failed to unmarshal multilinestring coordinates: %v", err)
	}

	polyline := &shp.PolyLine{}
	partIndex := int32(0)

	for _, line := range coords {
		polyline.Parts = append(polyline.Parts, partIndex)
		for _, coord := range line {
			if len(coord) >= 2 {
				polyline.Points = append(polyline.Points, shp.Point{X: coord[0], Y: coord[1]})
			}
		}
		partIndex = int32(len(polyline.Points))
	}

	shape.Write(polyline)
	return nil
}

// writeAttributesToShapefile writes feature properties as DBF attributes
func writeAttributesToShapefile(shape *shp.Writer, properties map[string]interface{}, fields []shp.Field, recordIndex int) error {
	for i, field := range fields {
		fieldName := string(field.Name[:])
		
		// Handle special ID field
		if fieldName == "ID" && len(properties) == 0 {
			shape.WriteAttribute(recordIndex, i, strconv.Itoa(recordIndex+1))
			continue
		}

		// Find matching property (case insensitive and truncated)
		var value interface{}
		found := false
		for propKey, propValue := range properties {
			if strings.EqualFold(propKey, fieldName) || 
			   (len(propKey) > 10 && strings.EqualFold(propKey[:10], fieldName)) {
				value = propValue
				found = true
				break
			}
		}

		if !found {
			// Use empty value for missing properties
			switch field.Fieldtype {
			case 'C': // Character/String
				shape.WriteAttribute(recordIndex, i, "")
			case 'N', 'F': // Numeric/Float
				shape.WriteAttribute(recordIndex, i, 0)
			default:
				shape.WriteAttribute(recordIndex, i, "")
			}
			continue
		}

		// Convert value to appropriate type
		switch field.Fieldtype {
		case 'C': // Character/String
			shape.WriteAttribute(recordIndex, i, fmt.Sprintf("%v", value))
		case 'N': // Numeric
			if numVal, ok := value.(float64); ok {
				shape.WriteAttribute(recordIndex, i, int(numVal))
			} else if intVal, ok := value.(int); ok {
				shape.WriteAttribute(recordIndex, i, intVal)
			} else {
				// Try to parse string as number
				if strVal, ok := value.(string); ok {
					if parsedInt, err := strconv.Atoi(strVal); err == nil {
						shape.WriteAttribute(recordIndex, i, parsedInt)
					} else {
						shape.WriteAttribute(recordIndex, i, 0)
					}
				} else {
					shape.WriteAttribute(recordIndex, i, 0)
				}
			}
		case 'F': // Float
			if numVal, ok := value.(float64); ok {
				shape.WriteAttribute(recordIndex, i, numVal)
			} else if intVal, ok := value.(int); ok {
				shape.WriteAttribute(recordIndex, i, float64(intVal))
			} else {
				// Try to parse string as float
				if strVal, ok := value.(string); ok {
					if parsedFloat, err := strconv.ParseFloat(strVal, 64); err == nil {
						shape.WriteAttribute(recordIndex, i, parsedFloat)
					} else {
						shape.WriteAttribute(recordIndex, i, 0.0)
					}
				} else {
					shape.WriteAttribute(recordIndex, i, 0.0)
				}
			}
		default:
			shape.WriteAttribute(recordIndex, i, fmt.Sprintf("%v", value))
		}
	}

	return nil
}