package sync

import (
	"encoding/json"
	"testing"

	"github.com/erichll/go-fast-note-sync/internal/local"
	"github.com/erichll/go-fast-note-sync/internal/state"
)

func TestNoteModifyPayloadBaseHash(t *testing.T) {
	t.Run("includes baseHash when file exists in FileHashMap", func(t *testing.T) {
		svc, conn, _ := newLocalEventService(t)
		svc.cfg.SyncEnabled = true
		svc.cfg.ManualSyncEnabled = false
		svc.cfg.ReadOnlySyncEnabled = false
		svc.st.IsInitSync = false
		svc.isSyncing = false
		relPath := "notes/test.md"
		writeVaultFile(t, svc.cfg.VaultPath, relPath, "new content")

		svc.st.FileHashMap[relPath] = state.FileHashEntry{
			Hash:  "old-base-hash",
			MTime: 123456789,
			Size:  10,
		}

		got := svc.HandleLocalModify(local.PathEvent{Path: relPath, IsDir: false})
		if !got.Attempted || got.Err != nil {
			t.Fatalf("HandleLocalModify result = %+v, want attempted success", got)
		}

		if len(conn.written) == 0 {
			t.Fatal("expected websocket write")
		}

		action, payloadJSON, ok := parseWSMessage(t, conn.written[0])
		if !ok || action != "NoteModify" {
			t.Fatalf("action = %s, want NoteModify", action)
		}

		var payload map[string]interface{}
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
			t.Fatalf("json unmarshal failed: %v", err)
		}

		if baseHash, ok := payload["baseHash"].(string); !ok || baseHash != "old-base-hash" {
			t.Errorf("baseHash = %v, want %q", payload["baseHash"], "old-base-hash")
		}
		if missing, ok := payload["baseHashMissing"].(bool); ok && missing {
			t.Errorf("baseHashMissing = %v, want false or absent", payload["baseHashMissing"])
		}
	})

	t.Run("sets baseHashMissing true when file is absent in FileHashMap", func(t *testing.T) {
		svc, conn, _ := newLocalEventService(t)
		svc.cfg.SyncEnabled = true
		svc.cfg.ManualSyncEnabled = false
		svc.cfg.ReadOnlySyncEnabled = false
		svc.st.IsInitSync = false
		svc.isSyncing = false
		relPath := "notes/new.md"
		writeVaultFile(t, svc.cfg.VaultPath, relPath, "new content")

		got := svc.HandleLocalModify(local.PathEvent{Path: relPath, IsDir: false})
		if !got.Attempted || got.Err != nil {
			t.Fatalf("HandleLocalModify result = %+v, want attempted success", got)
		}

		if len(conn.written) == 0 {
			t.Fatal("expected websocket write")
		}

		action, payloadJSON, ok := parseWSMessage(t, conn.written[0])
		if !ok || action != "NoteModify" {
			t.Fatalf("action = %s, want NoteModify", action)
		}

		var payload map[string]interface{}
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
			t.Fatalf("json unmarshal failed: %v", err)
		}

		if missing, ok := payload["baseHashMissing"].(bool); !ok || !missing {
			t.Errorf("baseHashMissing = %v, want true", payload["baseHashMissing"])
		}
		if baseHash, ok := payload["baseHash"]; ok && baseHash != nil && baseHash != "" {
			t.Errorf("baseHash = %v, want absent/empty", baseHash)
		}
	})

	t.Run("SettingModify does not include baseHash or baseHashMissing", func(t *testing.T) {
		svc, conn, _ := newLocalEventService(t)
		svc.cfg.SyncEnabled = true
		svc.cfg.ManualSyncEnabled = false
		svc.cfg.ReadOnlySyncEnabled = false
		svc.cfg.ConfigSyncEnabled = true
		svc.st.IsInitSync = false
		svc.isSyncing = false
		relPath := ".obsidian/app.json"
		writeVaultFile(t, svc.cfg.VaultPath, relPath, `{"theme":"dark"}`)

		svc.st.ConfigHashMap[relPath] = state.FileHashEntry{
			Hash:  "config-base-hash",
			MTime: 123456789,
			Size:  15,
		}

		got := svc.HandleLocalModify(local.PathEvent{Path: relPath, IsDir: false})
		if !got.Attempted || got.Err != nil {
			t.Fatalf("HandleLocalModify result = %+v, want attempted success", got)
		}

		if len(conn.written) == 0 {
			t.Fatal("expected websocket write")
		}

		action, payloadJSON, ok := parseWSMessage(t, conn.written[0])
		if !ok || action != "SettingModify" {
			t.Fatalf("action = %s, want SettingModify", action)
		}

		var payload map[string]interface{}
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
			t.Fatalf("json unmarshal failed: %v", err)
		}

		if _, ok := payload["baseHash"]; ok {
			t.Errorf("SettingModify should not have baseHash, got %v", payload["baseHash"])
		}
		if _, ok := payload["baseHashMissing"]; ok {
			t.Errorf("SettingModify should not have baseHashMissing, got %v", payload["baseHashMissing"])
		}
	})
}

func parseWSMessage(t *testing.T, raw string) (action, payloadJSON string, ok bool) {
	t.Helper()
	var actionStr string
	if idx := len(raw); idx > 0 {
		for i, ch := range raw {
			if ch == '|' {
				actionStr = raw[:i]
				payloadJSON = raw[i+1:]
				return actionStr, payloadJSON, true
			}
		}
	}
	return "", "", false
}
