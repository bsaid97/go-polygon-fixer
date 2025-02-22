package utils

import (
	"io"
	"mime/multipart"
	"net/http"
)

type MultipartResult struct {
	File       string
	Properties Properties
}

type Properties struct {
	FilePath          string
	SaveFile          bool
	FeatureCollection string
}

func ReadMultiPartForm(r *http.Request, fileKey string) MultipartResult {
	r.ParseMultipartForm(999999999999999)
	var fileHeader *multipart.FileHeader
	result := MultipartResult{
		File: "",
		Properties: Properties{
			FilePath:          "",
			SaveFile:          false,
			FeatureCollection: "",
		},
	}
	for key, value := range r.MultipartForm.File {
		if key == fileKey {
			fileHeader = value[0]
		}
	}

	for key, value := range r.MultipartForm.Value {
		if key == "filepath" {
			result.Properties.FilePath = value[0]
		}

		if key == "saveFile" {
			if value[0] == "true" {
				result.Properties.SaveFile = true
			} else {
				result.Properties.SaveFile = false
			}
		}

		if key == "featureCollection" {
			result.Properties.FeatureCollection = value[0]
		}
	}

	if fileHeader != nil {

		file, _ := fileHeader.Open()

		defer file.Close()

		fullFile, _ := io.ReadAll(file)

		result.File = string(fullFile)
	}

	return result
}
