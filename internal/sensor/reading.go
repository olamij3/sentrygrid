package sensor

import "time"

type Reading struct {
	DeviceID  string
	Timestamp time.Time
	Value     float64
}
