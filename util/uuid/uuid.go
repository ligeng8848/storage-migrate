package uuid

import (
	"strings"

	"github.com/gofrs/uuid"
)

func GenUUID() string {
	id, _ := uuid.NewV4()
	return strings.ReplaceAll(id.String(), "-", "")
}
