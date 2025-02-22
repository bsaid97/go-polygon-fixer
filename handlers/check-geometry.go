package handlers

import (
	"fmt"

	"github.com/twpayne/go-geos"
)

type Error struct {
	Ref          int    `json:"ref"`
	ErrorMessage string `json:"errorMessage"`
}

func CheckGeometry(geometryCollection *geos.Geom) []Error {
	var errors []Error

	fmt.Println("Found Geometries:", geometryCollection.NumGeometries())
	for i := range geometryCollection.NumGeometries() {
		shape := geometryCollection.Geometry(i)

		if !shape.IsValid() {
			reason := shape.IsValidReason()

			errorMessage := Error{Ref: i, ErrorMessage: reason}
			errors = append(errors, errorMessage)
		}
	}
	return errors
}
