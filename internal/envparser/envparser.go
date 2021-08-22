package envparser

import (
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
	"strings"
)

func LookupEnvOrString(key string, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}

func LookupEnvOrUint(key string, defaultVal uint) uint {
	if val, ok := os.LookupEnv(key); ok {
		v, err := strconv.ParseUint(val, 10, 32)
		if err != nil {
			log.Fatalf("LookupEnvOrInt[%s]: %v", key, err)
		}
		return uint(v)
	}
	return defaultVal
}

func LookupEnvOrBool(key string, defaultVal bool) bool {
	if val, ok := os.LookupEnv(key); ok {

		val = strings.ToLower(val)

		if val == "false" || val == "f" || val == "0" {
			return false
		} else {
			return true
		}
	}
	return defaultVal
}
