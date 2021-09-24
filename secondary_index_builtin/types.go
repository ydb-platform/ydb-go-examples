package main

import "time"

type User struct {
	ID   uint64
	Name string
	Age  uint32
}

type UsersList []User

type Series struct {
	ID             uint64
	Title          string
	Info           string
	ReleaseDate    time.Time
	Views          uint64
	UploadedUserID uint64
}

type SeriesList []Series
