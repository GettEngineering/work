package work

import "log"

func logError(key string, err error) {
	log.Printf("ERROR: %s - %s\n", key, err.Error())
}
