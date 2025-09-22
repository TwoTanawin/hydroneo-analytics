package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/TwoTanawin/sensor-queries/config"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	// Load .env
	err := godotenv.Load()
	if err != nil {
		log.Println("No .env file found, using environment variables only")
	}

	// Connect to MongoDB
	config.ConnectDB()

	// Get the collection
	collection := config.DB.Collection("sensor_resource_sensors")

	// Context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Empty filter -> fetch all, but limit to first 10
	filter := bson.M{
		"type": bson.M{
			"$in": []string{"PH", "DO"}, // list all types you want
		},
	}
	findOptions := options.Find()
	findOptions.SetLimit(3)
	findOptions.SetSkip(0)
	findOptions.SetSort(bson.M{"_id": 1})

	// Query
	cursor, err := collection.Find(ctx, filter, findOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer cursor.Close(ctx)

	// Collect results
	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		log.Fatal(err)
	}

	// Convert to JSON
	jsonData, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		log.Fatal(err)
	}

	// Print JSON
	fmt.Println(string(jsonData))
}
