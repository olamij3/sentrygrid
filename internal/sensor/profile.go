package sensor

import "time"

type Profile struct {
	Name          string
	BaseValue     float64
	NoiseStdDev   float64
	AnomalyChance float64
	DropoutChance float64
	DriftPerTick  float64
	TickInterval  time.Duration
}
