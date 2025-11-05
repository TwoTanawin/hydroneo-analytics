package dto

import "time"

type SensorMeasurementResponse struct {
	ID         string    `json:"id"`         // _id
	Class      string    `json:"class"`      // _class
	DeviceID   string    `json:"pId"`        // pId
	SensorID   string    `json:"sensorId"`   // sId
	SensorType string    `json:"sensorType"` // st
	Timestamp  time.Time `json:"timestamp"`  // t
	Value      *float64  `json:"value"`      // v
	Value2     *float64  `json:"value2"`     // v2 (optional)
	Value3     *float64  `json:"value3"`     // v3 (optional)
	Value4     *float64  `json:"value4"`     // v4 (optional)
	D          int       `json:"d"`          // d
	H          int       `json:"h"`          // h
	M          int       `json:"m"`          // m
	Year       int       `json:"year"`       // y
}
