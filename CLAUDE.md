# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Development Commands

- **Build the application**: `go build -o main .`
- **Run the application**: `./main` or `go run main.go`
- **Download dependencies**: `go mod download`
- **Clean up modules**: `go mod tidy`

## Docker Commands

- **Build Docker image**: `docker build -t go-polygon-fixer .`
- **Run Docker container**: `docker run -p 8080:8080 go-polygon-fixer`

## Application Architecture

This is a Go HTTP server that provides geometry processing services using the GEOS library. The application processes GeoJSON polygon data and provides three main endpoints:

### Core Structure

- **main.go**: HTTP server setup and main handlers
- **handlers/**: Contains specialized geometry processing functions
  - `check-geometry.go`: Validates geometries and returns error details
  - `dissolve.go`: Implements cascaded union operations for geometry collections
- **utils/**: Utility functions for geometry and request processing
  - `polygon-utils.go`: Coordinate truncation and polygon processing utilities
  - `request-utils.go`: Multipart form request handling

### Key Dependencies

- **github.com/twpayne/go-geos**: Go bindings for GEOS geometry library
- **github.com/twpayne/go-geom**: Geometry data structures
- Uses GEOS C library for geometric operations

### HTTP Endpoints

- `POST /dissolve`: Performs cascaded union on geometry collections
- `POST /check-geometry`: Validates geometries and returns validation errors
- `POST /v2/fix-geometry`: Fixes invalid geometries and optionally saves to file
- `POST /clean-topology`: Cleans topology gaps between adjacent polygons using snapping and validation

### Data Flow

1. Accepts GeoJSON as multipart form data or direct JSON payload
2. Parses into internal geometry structures using GEOS
3. Performs validation and/or geometric operations
4. Returns processed geometry as GeoJSON or saves to file

### Memory Management

The application uses explicit memory management with `geometry.Destroy()` calls to free GEOS geometry objects. This is critical when working with GEOS bindings to prevent memory leaks.

### Processing Features

- Coordinate truncation to 7 decimal places for precision control
- Concurrent processing of polygon geometries using goroutines
- Validation and repair of invalid geometries using GEOS MakeValid operations
- Support for both Polygon and MultiPolygon geometry types
- Topology cleaning using spatial indexing and boundary snapping
- Gap detection and elimination for polygon coverage datasets
- WGS84-optimized tolerance calculations for centimeter-level precision