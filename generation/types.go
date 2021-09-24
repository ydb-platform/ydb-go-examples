package main

import (
	_ "strconv"
	"time"
)

type User struct {
	ID       uint64
	Username string
	Mode     uint64    `ydb:"type:uint64?,conv:assert"`
	Magic    uint      `ydb:"type:uint32?,conv:unsafe"`
	Score    int64     `ydb:"type:int64?"`
	Updated  time.Time `ydb:"type:timestamp?"`
	Data     []byte    `ydb:"-"`
}

type Users []User

type MagicUsers struct {
	Magic uint     `ydb:"type:uint32?,conv:unsafe"`
	Users []string `ydb:"type:list<utf8>"`
}

type MagicUsersList []MagicUsers
