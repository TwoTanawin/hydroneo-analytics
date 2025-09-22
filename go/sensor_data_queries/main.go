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
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func main() {
	// Load .env
	_ = godotenv.Load()

	// Connect to MongoDB
	config.ConnectDB()

	collection := config.DB.Collection("sensor_resource_sensors")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Debug: print DB name and total docs
	fmt.Println("Database:", config.DB.Name())
	count, _ := collection.CountDocuments(ctx, bson.M{})
	fmt.Println("Total docs in collection:", count)

	idHex := "67b3039d85c10e3ee466eccd"
	objID, err := primitive.ObjectIDFromHex(idHex)
	if err != nil {
		log.Fatal("Invalid ObjectID:", err)
	}

	filter := bson.M{"_id": objID}

	var result bson.M
	err = collection.FindOne(ctx, filter).Decode(&result)
	if err != nil {
		log.Fatal("Sensor not found:", err)
	}

	jsonData, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println("Found sensor:", string(jsonData))
}
