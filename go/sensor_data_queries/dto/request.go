package dto

import "time"

// SensorDataRequest represents the query parameters for retrieving sensor measurements
type SensorDataRequest struct {
	SensorID string    `json:"sensorId" query:"sensorId" validate:"required"` // sId
	From     time.Time `json:"from,omitempty" query:"from"`                   // t >= from
	To       time.Time `json:"to,omitempty" query:"to"`                       // t <= to
}
