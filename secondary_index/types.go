package main

import "time"

type Series struct {
	ID          uint64
	Title       string
	Info        string
	ReleaseDate time.Time
	Views       uint64
}

type SeriesList []Series
