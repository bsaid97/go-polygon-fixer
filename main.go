package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/bsaid97/go-polygon-fixer/handlers"
	"github.com/bsaid97/go-polygon-fixer/utils"
	"github.com/twpayne/go-geos"
)

type FileNameRequest struct {
	OutputFile bool
	Filename   string
}

type Coord struct {
	X float64
	Y float64
}

// Geometry struct: Only MultiPolygon
type MultiPolygonGeometry struct {
	Type        string          `json:"type"`
	Coordinates [][][][]float64 `json:"coordinates"` // MultiPolygon: [][][][lon, lat]
}

// Geometry struct: Only MultiPolygon
type Geometry struct {
	Type        string          `json:"type"`
	Coordinates [][][][]float64 `json:"coordinates"` // MultiPolygon: [][][][lon, lat]
}

// Geometry struct: Only Polygon
type PolygonGeometry struct {
	Type        string          `json:"type"`
	Coordinates [][][][]float64 `json:"coordinates"` // Polygon: [][][lon, lat]
}

// Feature struct: Holds geometry + properties
type Feature struct {
	Type       string                 `json:"type"`
	Geometry   json.RawMessage        `json:"geometry"`
	Properties map[string]interface{} `json:"properties"`
}

// FeatureCollection struct: Holds multiple features
type FeatureCollection struct {
	Type     string    `json:"type"`
	Features []Feature `json:"features"`
}

type GeomFeature struct {
	Geom       *geos.Geom
	Properties map[string]interface{}
}

func main() {
	log.Printf("=== Starting Go Polygon Fixer Server ===")
	
	// Register handlers
	http.HandleFunc("/dissolve", dissolveHandler)
	http.HandleFunc("/check-geometry", checkGeometryHandler)
	// http.HandleFunc("/fix-geometry", fixGeometryHandler)
	http.HandleFunc("/v2/fix-geometry", fixGeometryHandler2)
	http.HandleFunc("/clean-topology", cleanTopologyHandler)
	
	log.Printf("Registered all HTTP handlers")
	
	// Start the HTTP server on port 8080
	log.Printf("Server is listening on port 8080...")
	fmt.Println("Server is listening on port 8080...")
	
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}

func readBody(w http.ResponseWriter, r *http.Request) string {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method, only POST allowed", http.StatusMethodNotAllowed)
		// return
	}

	// Read the request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		// return
	}
	defer r.Body.Close()

	return string(body)
}

func fixGeometryHandler2(w http.ResponseWriter, r *http.Request) {
	multiPartRequest := utils.ReadMultiPartForm(r, "file")
	var geometryPayload string
	fmt.Print("Request Received.")

	if multiPartRequest.File == "" {
		if multiPartRequest.Properties.FeatureCollection != "" {
			geometryPayload = multiPartRequest.Properties.FeatureCollection
		} else if multiPartRequest.Properties.FilePath != "" {
			geometryPayload = readFile(multiPartRequest.Properties)
		} else {
			sendResponse(w, []byte("ERROR: No suitable files found"))
		}
	} else {
		fmt.Println("Reading from payload")
		geometryPayload = multiPartRequest.File
	}

	var featureCollection FeatureCollection
	var geomFeatures []GeomFeature
	json.Unmarshal([]byte(geometryPayload), &featureCollection)
	var err error

	for i := range len(featureCollection.Features) {
		jsonString, _ := json.Marshal(featureCollection.Features[i].Geometry)
		geo, _ := geos.NewGeomFromGeoJSON(string(jsonString))

		if !geo.IsValid() {
			fmt.Println(featureCollection.Features[i].Properties["PC6"], geo.IsValidReason())
			geo = geo.MakeValidWithParams(geos.MakeValidLinework, geos.MakeValidDiscardCollapsed)
			geo, err = utils.TruncateFullGeometry(geo)

			if err != nil {
				fmt.Println("ERROR Trunc", featureCollection.Features[i].Properties["PC6"])
			}
		} else {
			geo, err = utils.TruncateFullGeometry(geo)
			if err != nil {
				fmt.Println("ERROR Trunc", featureCollection.Features[i].Properties["PC6"])
			}
		}

		if !geo.IsValid() {
			// fmt.Println(featureCollection.Features[i].Properties["PC6"], "after trunc", geo.IsValidReason())
			geo = geo.MakeValidWithParams(geos.MakeValidLinework, geos.MakeValidDiscardCollapsed)
			if err != nil {
				fmt.Println("ERROR Trunc", featureCollection.Features[i].Properties["PC6"])
			}
		}

		if geo.TypeID() == 6 || geo.TypeID() == 3 {
			geomFeature := GeomFeature{
				Geom:       geo,
				Properties: featureCollection.Features[i].Properties,
			}

			geomFeatures = append(geomFeatures, geomFeature)
		}
	}
	finalFeatureCollection := FeatureCollection{
		Features: make([]Feature, 0),
		Type:     "FeatureCollection",
	}
	for i := range len(geomFeatures) {
		geomFeature := geomFeatures[i]

		jsonString := geomFeature.Geom.ToGeoJSON(-1)

		feature := Feature{
			Type:       "Feature",
			Properties: geomFeature.Properties,
			Geometry:   json.RawMessage(jsonString),
		}

		finalFeatureCollection.Features = append(finalFeatureCollection.Features, feature)
	}
	jsonFC, _ := json.Marshal(finalFeatureCollection)

	if multiPartRequest.Properties.SaveFile {
		saveFile(multiPartRequest.Properties.FilePath, string(jsonFC))
		sendResponse(w, []byte("File Saved"))
	} else {
		fmt.Println("Done. Sending Response")
		sendResponse(w, []byte(jsonFC))
	}
}

// func fixGeometryHandler(w http.ResponseWriter, r *http.Request) {
// 	requestBody := readBody(w, r)
// 	var geometryPayload string
// 	var request FileNameRequest
// 	if strings.Contains(requestBody, "filename") {
// 		json.Unmarshal([]byte(requestBody), &request)
// 		geometryPayload = readFile(request)
// 	} else {
// 		geometryPayload = requestBody
// 	}

// 	geo1, err := geos.NewGeomFromGeoJSON(geometryPayload)
// 	if err != nil {
// 		log.Fatalf("Error creating geom")
// 	}
// 	// geo1 = geo1.MakeValid()

// 	// truncatedFeature, _ := utils.TruncateFullGeometry(geo1)

// 	// fmt.Println("Geometry Fixed", truncatedFeature.IsValidReason())

// 	jsonFeature := geo1.MakeValidWithParams(geos.MakeValidLinework, geos.MakeValidDiscardCollapsed).ToGeoJSON(-1)
// 	if request.OutputFile {
// 		saveFile(request.Filename, jsonFeature)
// 		sendResponse(w, []byte("File Saved"))
// 	} else {
// 		sendResponse(w, []byte(jsonFeature))
// 	}
// 	geo1.Destroy()
// 	// truncatedFeature.Destroy()
// }

func saveFile(filePath string, jsonString string) {
	name := strings.Replace(filePath, ".json", "", 1)
	name = strings.Replace(name, "files", "output", 1)
	filename := name + "_PROCESSED.json"

	err := os.WriteFile(filename, []byte(jsonString), 0644)
	if err != nil {
		fmt.Println("Error saving JSON file:", err)
		return
	}

	fmt.Println("JSON saved to", filename)
}

func readFile(filePath utils.Properties) string {
	file, _ := os.ReadFile(filePath.FilePath)

	return string(file)
}

func dissolveHandler(w http.ResponseWriter, r *http.Request) {
	// Assume geo1 is your GeometryCollection
	geo1, err := geos.NewGeomFromGeoJSON(readBody(w, r))
	if err != nil {
		log.Fatalf("Error creating geom")
	}
	numGeometries := geo1.NumGeometries()
	geometries := make([]*geos.Geom, numGeometries)
	fmt.Println("isSimple", geo1.IsValidReason())
	geo1 = geo1.MakeValid()
	for i := 0; i < numGeometries; i++ {
		geometries[i] = geo1.Geometry(i).Buffer(0, 0)
	}

	// Use the cascaded union approach
	finalUnion, err := handlers.CascadedUnion(geometries)

	if finalUnion == nil {
		log.Fatalf("Error can't find final Union")
	}

	if err != nil {
		log.Fatalf("Error performing cascaded union: %v", err)
	}

	truncatedFeature, _ := utils.TruncateFullGeometry(finalUnion)

	if err != nil {
		fmt.Println("error", err)
	}
	// Use finalUnion as needed
	fmt.Println("Union complete", truncatedFeature.IsValidReason())
	jsonFeature := truncatedFeature.MakeValidWithParams(geos.MakeValidStructure, geos.MakeValidDiscardCollapsed).ToGeoJSON(-1)
	sendResponse(w, []byte(jsonFeature))
	finalUnion.Destroy()
}

func checkGeometryHandler(w http.ResponseWriter, r *http.Request) {
	geo1, err := geos.NewGeomFromGeoJSON(readBody(w, r))
	if err != nil {
		log.Fatalf("Error creating geom")
	}
	errors := handlers.CheckGeometry(geo1)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(errors)
}

func cleanTopologyHandler(w http.ResponseWriter, r *http.Request) {
	// Add panic recovery to prevent server crashes
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC recovered in cleanTopologyHandler: %v", r)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
	}()
	
	log.Printf("=== Topology cleaning request received ===")
	log.Printf("Content-Type: %s", r.Header.Get("Content-Type"))
	
	var geometryPayload string
	
	// Check if this is a direct JSON request or multipart form
	contentType := r.Header.Get("Content-Type")
	if strings.Contains(contentType, "application/json") {
		// Handle direct JSON request
		log.Printf("Handling direct JSON request")
		geometryPayload = readBody(w, r)
		if geometryPayload == "" {
			sendResponse(w, []byte("ERROR: Empty request body"))
			return
		}
	} else {
		// Handle multipart form request
		log.Printf("Handling multipart form request")
		multiPartRequest := utils.ReadMultiPartForm(r, "file")
		
		if multiPartRequest.File == "" {
			if multiPartRequest.Properties.FeatureCollection != "" {
				geometryPayload = multiPartRequest.Properties.FeatureCollection
			} else if multiPartRequest.Properties.FilePath != "" {
				geometryPayload = readFile(multiPartRequest.Properties)
			} else {
				sendResponse(w, []byte("ERROR: No suitable files found"))
				return
			}
		} else {
			log.Printf("Reading from uploaded file")
			geometryPayload = multiPartRequest.File
		}
	}

	// Check if shapefile format is requested (you can add a parameter for this)
	// For now, always generate zip with both formats
	zipData, err := handlers.CleanTopologyWithShapefile(geometryPayload)
	if err != nil {
		http.Error(w, fmt.Sprintf("ERROR: Topology cleaning failed: %v", err), http.StatusInternalServerError)
		return
	}

	// For direct JSON requests, always return the zip response
	// For multipart requests, check if file saving is requested
	if strings.Contains(contentType, "application/json") {
		log.Printf("Topology cleaning complete. Sending zip response")
		sendZipResponse(w, zipData)
	} else {
		// This is a multipart form request, check if saving is requested
		multiPartRequest := utils.ReadMultiPartForm(r, "file")
		if multiPartRequest.Properties.SaveFile {
			saveZipFile(multiPartRequest.Properties.FilePath, zipData)
			sendResponse(w, []byte("Topology cleaned and zip file saved"))
		} else {
			log.Printf("Topology cleaning complete. Sending zip response")
			sendZipResponse(w, zipData)
		}
	}
}

func sendResponse(w http.ResponseWriter, response []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

func sendZipResponse(w http.ResponseWriter, zipData []byte) {
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", "attachment; filename=\"cleaned_topology.zip\"")
	w.WriteHeader(http.StatusOK)
	w.Write(zipData)
}

func saveZipFile(filePath string, zipData []byte) {
	name := strings.Replace(filePath, ".json", "", 1)
	name = strings.Replace(name, "files", "output", 1)
	filename := name + "_PROCESSED.zip"

	err := os.WriteFile(filename, zipData, 0644)
	if err != nil {
		fmt.Println("Error saving zip file:", err)
		return
	}

	fmt.Println("Zip file saved to", filename)
}
