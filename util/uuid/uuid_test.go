package uuid

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenUUID(t *testing.T) {
	got := GenUUID()
	assert.NotNil(t, got)
}
