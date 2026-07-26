package store

import (
	"context"
	"database/sql"
	_ "embed"
	"time"

	_ "modernc.org/sqlite"

	"github.com/olamij3/sentrygrid/internal/pool"
	"github.com/olamij3/sentrygrid/internal/sensor"
)

//go:embed schema.sql
var schema string

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(schema); err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) InsertReading(r sensor.Reading) error {
	_, err := s.db.Exec(
		`INSERT INTO readings (device_id, timestamp, value) VALUES (?, ?, ?)`,
		r.DeviceID, r.Timestamp, r.Value,
	)
	return err
}

func (s *Store) InsertAnomaly(a pool.Anomaly) error {
	_, err := s.db.Exec(
		`INSERT INTO anomalies (device_id, timestamp, value, method, score) VALUES (?, ?, ?, ?, ?)`,
		a.DeviceID, a.Timestamp, a.Value, a.Method, a.Score,
	)
	return err
}

func (s *Store) RecentReadings(deviceID string, limit int) ([]sensor.Reading, error) {
	rows, err := s.db.Query(
		`SELECT device_id, timestamp, value FROM readings
		 WHERE device_id = ? ORDER BY timestamp DESC LIMIT ?`,
		deviceID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var readings []sensor.Reading
	for rows.Next() {
		var r sensor.Reading
		var ts string
		if err := rows.Scan(&r.DeviceID, &ts, &r.Value); err != nil {
			return nil, err
		}
		r.Timestamp, _ = time.Parse("2006-01-02T15:04:05.999999999Z07:00", ts)
		readings = append(readings, r)
	}
	return readings, nil
}

func (s *Store) RecentAnomalies(limit int) ([]pool.Anomaly, error) {
	rows, err := s.db.Query(
		`SELECT device_id, timestamp, value, method, score FROM anomalies
		 ORDER BY timestamp DESC LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var anomalies []pool.Anomaly
	for rows.Next() {
		var a pool.Anomaly
		var ts string
		if err := rows.Scan(&a.DeviceID, &ts, &a.Value, &a.Method, &a.Score); err != nil {
			return nil, err
		}
		a.Timestamp, _ = time.Parse("2006-01-02T15:04:05.999999999Z07:00", ts)
		anomalies = append(anomalies, a)
	}
	return anomalies, nil
}

func (s *Store) RunWriter(ctx context.Context, readings <-chan sensor.Reading, anomalies <-chan pool.Anomaly) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case r, ok := <-readings:
				if !ok {
					return
				}
				_ = s.InsertReading(r)
			}
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case a, ok := <-anomalies:
				if !ok {
					return
				}
				_ = s.InsertAnomaly(a)
			}
		}
	}()
}