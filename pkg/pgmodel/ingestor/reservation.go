package ingestor

import "time"

type reservation struct {
	copySender <-chan copyRequest
	startTime  time.Time
}
