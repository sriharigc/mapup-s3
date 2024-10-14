package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time" // Import time package

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Helper function to convert numeric month to month name
func getMonthName(month string) (string, error) {
	// Parse the numeric month string (e.g., "04") into a time.Month type
	parsedTime, err := time.Parse("01", month) // "01" is the layout for parsing month
	if err != nil {
		return "", fmt.Errorf("invalid month format: %v", err)
	}

	// Return the full month name (e.g., "April")
	return parsedTime.Month().String(), nil
}

// Function to download the S3 object
func downloadObject(s3Client *s3.Client, bucket, key string) (string, error) {
	output, err := s3Client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", err
	}
	defer output.Body.Close()

	tmpFile, err := os.CreateTemp("", "downloaded_gps_data_*.json")
	if err != nil {
		return "", err
	}
	defer tmpFile.Close()

	_, err = io.Copy(tmpFile, output.Body)
	if err != nil {
		return "", err
	}

	return tmpFile.Name(), nil
}

// HTTP handler function for the GET request
func handleGet(w http.ResponseWriter, r *http.Request, s3Client *s3.Client) {
	userID := r.URL.Query().Get("user_id")
	vehicleID := r.URL.Query().Get("vehicle_id")
	year := r.URL.Query().Get("year")
	month := r.URL.Query().Get("month")
	day := r.URL.Query().Get("day")

	// Validate query parameters
	if userID == "" || vehicleID == "" || year == "" || month == "" || day == "" {
		http.Error(w, "Missing query parameters", http.StatusBadRequest)
		return
	}

	// Convert numeric month to month name using the helper function
	monthName, err := getMonthName(month)
	if err != nil {
		http.Error(w, "Invalid month format", http.StatusBadRequest)
		return
	}

	// Construct the S3 key dynamically using the month name
	keyParts := []string{
		"gps_data-20241008T164836Z-001",
		"gps_data",
		"dev",
		userID,
		vehicleID,
		year,
		monthName, // Using the month name
		day,
		"gps_data.json",
	}
	key := strings.Join(keyParts, "/")

	// Define your S3 bucket name
	bucket := "srihari03"

	// Attempt to download the object
	filePath, err := downloadObject(s3Client, bucket, key)
	if err != nil {
		log.Printf("Failed to retrieve object from S3: %v", err)
		http.Error(w, "Failed to retrieve object", http.StatusInternalServerError)
		return
	}

	// Read the contents of the downloaded file and write it to the response
	file, err := os.ReadFile(filePath)
	if err != nil {
		log.Printf("Failed to read file: %v", err)
		http.Error(w, "Failed to read downloaded file", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(file)
}

func main() {
	// Load the AWS configuration
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("ap-south-1"))
	if err != nil {
		log.Fatalf("Unable to load SDK config, %v", err)
	}

	// Create an S3 client
	s3Client := s3.NewFromConfig(cfg)

	// Set up the HTTP handler
	http.HandleFunc("/get-data", func(w http.ResponseWriter, r *http.Request) {
		handleGet(w, r, s3Client)
	})

	// Start the HTTP server
	log.Println("Server is starting on port 8090...")
	if err := http.ListenAndServe(":8090", nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
