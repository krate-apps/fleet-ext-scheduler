// Command scheduler-worker applies bandwidth schedules via fleet.throttle.* (public SDK).
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      any             `json:"id"`
}

// slot is a weekly window; days use Go weekday numbers (0=Sunday … 6=Saturday).
type slot struct {
	Days            []int  `json:"days"`
	StartHour       int    `json:"startHour"`
	EndHour         int    `json:"endHour"`
	DownRate        int64  `json:"downRate"`
	UpRate          int64  `json:"upRate"`
	ThrottleProfile string `json:"throttleProfile,omitempty"`
}

type schedule struct {
	Enabled bool   `json:"enabled"`
	Slots   []slot `json:"slots"`
}

var (
	mu       sync.Mutex
	nextHost = int64(1)
	pending  = map[int64]chan hostReply{}
	outMu    sync.Mutex
)

type hostReply struct {
	result json.RawMessage
	err    string
}

func main() {
	go loopApply()

	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 0, 64*1024), 8<<20)
	for in.Scan() {
		line := in.Bytes()
		if len(line) == 0 {
			continue
		}
		var probe map[string]json.RawMessage
		if err := json.Unmarshal(line, &probe); err != nil {
			continue
		}
		if _, hasMethod := probe["method"]; !hasMethod {
			var resp struct {
				ID     any             `json:"id"`
				Result json.RawMessage `json:"result"`
				Error  *struct {
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal(line, &resp); err != nil {
				continue
			}
			idNum, ok := asInt64(resp.ID)
			if !ok {
				continue
			}
			mu.Lock()
			ch := pending[idNum]
			delete(pending, idNum)
			mu.Unlock()
			if ch == nil {
				continue
			}
			msg := ""
			if resp.Error != nil {
				msg = resp.Error.Message
			}
			ch <- hostReply{result: resp.Result, err: msg}
			continue
		}

		var req request
		if err := json.Unmarshal(line, &req); err != nil {
			continue
		}
		go handleRequest(req)
	}
}

func handleRequest(req request) {
	switch req.Method {
	case "fleet.handshake", "fleet.ping", "ext.activate", "ext.deactivate", "ext.health", "ext.upgrade":
		reply(req.ID, map[string]any{"ok": true})
	case "fleet.shutdown":
		reply(req.ID, map[string]any{"ok": true})
		os.Exit(0)
	case "ext.scheduler.get":
		st, err := loadSchedule()
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, st)
	case "ext.scheduler.save":
		var st schedule
		if err := json.Unmarshal(req.Params, &st); err != nil {
			replyErr(req.ID, "invalid schedule")
			return
		}
		if err := validateSchedule(st); err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		if err := saveSchedule(&st); err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, st)
	case "ext.scheduler.apply":
		applied, err := applyNow()
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, applied)
	case "ext.scheduler.status":
		st, err := loadSchedule()
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		rates, _ := hostCall("fleet.throttle.get", map[string]any{})
		slot := matchSlot(st, time.Now())
		reply(req.ID, map[string]any{
			"schedule": st,
			"active":   slot,
			"throttle": json.RawMessage(rates),
			"now":      time.Now().UTC().Format(time.RFC3339),
		})
	default:
		replyErr(req.ID, fmt.Sprintf("method not found: %s", req.Method))
	}
}

func loopApply() {
	time.Sleep(5 * time.Second)
	for {
		st, err := loadSchedule()
		if err == nil && st.Enabled {
			_, _ = applyNow()
		}
		time.Sleep(60 * time.Second)
	}
}

func validateSchedule(st schedule) error {
	for i, s := range st.Slots {
		if s.StartHour < 0 || s.StartHour > 23 || s.EndHour < 0 || s.EndHour > 24 {
			return fmt.Errorf("slots[%d]: invalid hours", i)
		}
		if s.DownRate < 0 || s.UpRate < 0 {
			return fmt.Errorf("slots[%d]: rates must be >= 0", i)
		}
		for _, d := range s.Days {
			if d < 0 || d > 6 {
				return fmt.Errorf("slots[%d]: invalid day %d", i, d)
			}
		}
	}
	return nil
}

func applyNow() (map[string]any, error) {
	st, err := loadSchedule()
	if err != nil {
		return nil, err
	}
	if !st.Enabled {
		return map[string]any{"ok": true, "applied": false, "reason": "disabled"}, nil
	}
	slot := matchSlot(st, time.Now())
	if slot == nil {
		return map[string]any{"ok": true, "applied": false, "reason": "no matching slot"}, nil
	}
	profile := strings.TrimSpace(slot.ThrottleProfile)
	if profile != "" {
		if _, err := hostCall("fleet.throttle.define", map[string]any{
			"name":     profile,
			"upRate":   slot.UpRate,
			"downRate": slot.DownRate,
		}); err != nil {
			return nil, fmt.Errorf("define profile: %w", err)
		}
	}
	if _, err := hostCall("fleet.throttle.set", map[string]any{
		"downRate": slot.DownRate,
		"upRate":   slot.UpRate,
	}); err != nil {
		return nil, err
	}
	return map[string]any{
		"ok":              true,
		"applied":         true,
		"slot":            slot,
		"throttleProfile": profile,
	}, nil
}

func matchSlot(st *schedule, now time.Time) *slot {
	if st == nil {
		return nil
	}
	wd := int(now.Weekday())
	hour := now.Hour()
	for i := range st.Slots {
		s := &st.Slots[i]
		dayOK := len(s.Days) == 0
		for _, d := range s.Days {
			if d == wd {
				dayOK = true
				break
			}
		}
		if !dayOK {
			continue
		}
		end := s.EndHour
		if end == 0 && s.StartHour != 0 {
			end = 24
		}
		if hour >= s.StartHour && hour < end {
			return s
		}
		// Overnight window e.g. 22→6
		if s.StartHour > end && (hour >= s.StartHour || hour < end) {
			return s
		}
	}
	return nil
}

func loadSchedule() (*schedule, error) {
	raw, err := hostCall("fleet.storage.get", map[string]any{"key": "schedule"})
	if err != nil {
		return nil, err
	}
	if string(raw) == "null" || len(raw) == 0 {
		return &schedule{Enabled: false, Slots: []slot{}}, nil
	}
	var st schedule
	if err := json.Unmarshal(raw, &st); err != nil {
		return &schedule{Enabled: false, Slots: []slot{}}, nil
	}
	return &st, nil
}

func saveSchedule(st *schedule) error {
	_, err := hostCall("fleet.storage.put", map[string]any{"key": "schedule", "value": st})
	return err
}

func hostCall(method string, params any) (json.RawMessage, error) {
	mu.Lock()
	id := nextHost
	nextHost++
	ch := make(chan hostReply, 1)
	pending[id] = ch
	mu.Unlock()
	req := map[string]any{"jsonrpc": "2.0", "method": method, "params": params, "id": id}
	line, _ := json.Marshal(req)
	line = append(line, '\n')
	outMu.Lock()
	_, err := os.Stdout.Write(line)
	outMu.Unlock()
	if err != nil {
		return nil, err
	}
	select {
	case rep := <-ch:
		if rep.err != "" {
			return nil, fmt.Errorf("%s", rep.err)
		}
		if len(rep.result) == 0 {
			return json.RawMessage("null"), nil
		}
		return rep.result, nil
	case <-time.After(30 * time.Second):
		mu.Lock()
		delete(pending, id)
		mu.Unlock()
		return nil, fmt.Errorf("host call timeout")
	}
}

func reply(id any, result any) {
	outMu.Lock()
	defer outMu.Unlock()
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

func replyErr(id any, msg string) {
	outMu.Lock()
	defer outMu.Unlock()
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error":   map[string]any{"code": -32000, "message": msg},
	})
}

func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	default:
		return 0, false
	}
}
