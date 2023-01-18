package main

import (
	"gorm.io/gorm/clause"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Series struct {
	ID          string    `gorm:"column:series_id;primarykey;not null"`
	Title       string    `gorm:"column:title;not null"`
	Info        string    `gorm:"column:series_info"`
	Comment     string    `gorm:"column:comment"`
	ReleaseDate time.Time `gorm:"column:release_date;not null"`

	Seasons []Season
}

func (s *Series) BeforeCreate(tx *gorm.DB) (err error) {
	id, err := uuid.NewUUID()
	if err != nil {
		return err
	}
	s.ID = id.String()
	for _, season := range s.Seasons {
		season.SeriesID = s.ID
	}
	return
}

func (s *Series) GetSeasons(tx *gorm.DB) ([]Season, error) {
	if len(s.Seasons) > 0 {
		return s.Seasons, nil
	}
	return s.Seasons, tx.Order("first_aired").Find(&s.Seasons, clause.Eq{
		Column: "series_id",
		Value:  s.ID,
	}).Error
}

type Season struct {
	ID         string    `gorm:"column:season_id;primarykey"`
	SeriesID   string    `gorm:"column:series_id;index"`
	Title      string    `gorm:"column:title;not null"`
	FirstAired time.Time `gorm:"column:first_aired;not null"`
	LastAired  time.Time `gorm:"column:last_aired;not null"`

	Episodes []Episode
}

func (s *Season) BeforeCreate(tx *gorm.DB) (err error) {
	id, err := uuid.NewUUID()
	if err != nil {
		return err
	}
	s.ID = id.String()
	for _, episode := range s.Episodes {
		episode.SeasonID = s.ID
	}
	return
}

func (s *Season) GetEpisodes(tx *gorm.DB) (episodes []Episode, _ error) {
	if len(s.Episodes) > 0 {
		return s.Episodes, nil
	}
	return s.Episodes, tx.Order("air_date").Find(&s.Episodes, clause.Eq{
		Column: "season_id",
		Value:  s.ID,
	}).Error
}

type Episode struct {
	ID       string    `gorm:"column:episode_id;primarykey"`
	SeasonID string    `gorm:"column:season_id;index;not null"`
	Title    string    `gorm:"column:title;not null"`
	AirDate  time.Time `gorm:"column:air_date;not null"`
}

func (e *Episode) BeforeCreate(tx *gorm.DB) (err error) {
	id, err := uuid.NewUUID()
	if err != nil {
		return err
	}
	e.ID = id.String()
	return
}
