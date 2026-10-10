package cleanup

import (
	"time"

	"gorm.io/gorm"
)

type Opts struct {
	DB         *gorm.DB
	Retention  time.Duration
	Interval   time.Duration
	BatchLimit int
	Duplicates time.Duration
	OnError    func(error)
}
