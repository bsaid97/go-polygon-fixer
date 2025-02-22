package handlers

import "github.com/twpayne/go-geos"

func CascadedUnion(geometries []*geos.Geom) (*geos.Geom, error) {
	// Base case: if there is only one geometry, return it
	if len(geometries) == 1 {
		return geometries[0], nil
	}

	// Divide the array into two halves
	mid := len(geometries) / 2
	left, err := CascadedUnion(geometries[:mid])
	if err != nil {
		return nil, err
	}
	right, err := CascadedUnion(geometries[mid:])
	if err != nil {
		return nil, err
	}

	// Union the results of the left and right halves
	result := left.Union(right)

	// Clean up to free memory
	left.Destroy()
	right.Destroy()

	return result, nil
}
