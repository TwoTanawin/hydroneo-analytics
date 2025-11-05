package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/TwoTanawin/sensor-queries/config"
	"github.com/TwoTanawin/sensor-queries/dto"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func main() {
	// Load .env
	_ = godotenv.Load()

	// Connect to MongoDB
	config.ConnectDB()

	sensor_collection := config.DB.Collection("sensor_resource_measurements")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Debug: print DB name and total docs
	fmt.Println("Database:", config.DB.Name())
	count, _ := sensor_collection.CountDocuments(ctx, bson.M{})
	fmt.Println("Total docs in sensor_resource_measurements:", count)

	// Example sensor ID and timestamp range
	idHex := "67b3039d85c10e3ee466eccd"
	objID, err := primitive.ObjectIDFromHex(idHex)
	if err != nil {
		log.Fatal("Invalid ObjectID:", err)
	}

	from := time.Date(2025, 4, 21, 0, 0, 0, 0, time.UTC)
	to := time.Date(2025, 4, 21, 23, 59, 59, 0, time.UTC)

	// Build filter
	filter := bson.M{"sId": objID}
	if !from.IsZero() && !to.IsZero() {
		filter["t"] = bson.M{
			"$gte": from,
			"$lte": to,
		}
	}

	// Query multiple documents
	cursor, err := sensor_collection.Find(ctx, filter)
	if err != nil {
		log.Fatal("Error querying sensors:", err)
	}
	defer cursor.Close(ctx)

	// Iterate results and map to DTO
	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		log.Fatal("Error reading cursor:", err)
	}

	var response []dto.SensorMeasurementResponse
	for _, r := range results {
		// Parse timestamp
		var timestamp time.Time
		switch t := r["t"].(type) {
		case primitive.DateTime:
			timestamp = t.Time()
		case time.Time:
			timestamp = t
		case string:
			timestamp, _ = time.Parse(time.RFC3339, t)
		}

		// Helper to convert optional float fields
		getFloatPtr := func(key string) *float64 {
			if val, ok := r[key]; ok {
				switch f := val.(type) {
				case float64:
					return &f
				case int32:
					v := float64(f)
					return &v
				case int:
					v := float64(f)
					return &v
				}
			}
			return nil
		}

		getIDStr := func(val interface{}) string {
			switch v := val.(type) {
			case primitive.ObjectID:
				return v.Hex()
			case string:
				return v
			default:
				return ""
			}
		}

		response = append(response, dto.SensorMeasurementResponse{
			ID:         r["_id"].(primitive.ObjectID).Hex(),
			Class:      r["_class"].(string),
			DeviceID:   getIDStr(r["pId"]),
			SensorID:   getIDStr(r["sId"]),
			SensorType: r["st"].(string),
			Timestamp:  timestamp,
			Value:      getFloatPtr("v"),
			Value2:     getFloatPtr("v2"),
			Value3:     getFloatPtr("v3"),
			Value4:     getFloatPtr("v4"),
			D:          int(r["d"].(int32)),
			H:          int(r["h"].(int32)),
			M:          int(r["m"].(int32)),
			Year:       int(r["y"].(int32)),
		})

	}

	jsonData, _ := json.MarshalIndent(response, "", "  ")
	fmt.Println("Found sensors:", string(jsonData))
}
