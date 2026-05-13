package telegram

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseRemindDuration(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	runAt, text, err := parseReminder("/remind 10m drink water", now)
	require.NoError(t, err)
	require.Equal(t, now.Add(10*time.Minute), runAt)
	require.Equal(t, "drink water", text)
}

func TestParseRemindTimestamp(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	runAt, text, err := parseReminder("/remind 2026-05-02T18:00:00Z check something", now)
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, 5, 2, 18, 0, 0, 0, time.UTC), runAt)
	require.Equal(t, "check something", text)
}
