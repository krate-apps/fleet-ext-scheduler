package main

import (
	"testing"
	"time"
)

func TestMatchSlotOvernight(t *testing.T) {
	st := &schedule{Slots: []slot{{
		Days:      []int{int(time.Monday)},
		StartHour: 22,
		EndHour:   6,
		UpRate:    100,
		DownRate:  200,
	}}}
	// Monday 23:00
	now := time.Date(2026, 11, 9, 23, 0, 0, 0, time.UTC) // Monday
	if now.Weekday() != time.Monday {
		t.Fatalf("fixture weekday %v", now.Weekday())
	}
	s := matchSlot(st, now)
	if s == nil {
		t.Fatal("expected overnight match")
	}
	// Monday 10:00 — outside window
	day := time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)
	if matchSlot(st, day) != nil {
		t.Fatal("expected no match at 10:00")
	}
}

func TestMatchSlotEmptyDays(t *testing.T) {
	st := &schedule{Slots: []slot{{
		StartHour:       0,
		EndHour:         24,
		ThrottleProfile: "night",
	}}}
	if matchSlot(st, time.Now()) == nil {
		t.Fatal("empty days should match all")
	}
}
