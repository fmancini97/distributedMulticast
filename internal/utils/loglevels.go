package utils

import log "github.com/sirupsen/logrus"

func GetLogLevels() []string {
	levels := make([]string, 0)
	for _, level := range log.AllLevels {
		levels = append(levels, level.String())
	}

	return levels
}
