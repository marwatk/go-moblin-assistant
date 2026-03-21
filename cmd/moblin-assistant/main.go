package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	moblin "github.com/marwatk/go-moblin-assistant"
)

// AppState holds the latest telemetry data received from the app.
type AppState struct {
	sync.RWMutex
	LastState  *moblin.StreamerState
	LastStatus *moblin.StatusPayload
}

// rawLog controls whether raw JSON payloads are printed before parsing.
var rawLog bool

func main() {
	mux := http.NewServeMux()
	appState := &AppState{}

	pass := os.Getenv("MOBLIN_REMOTE_PASS")
	if pass == "" {
		log.Fatal("FATAL: MOBLIN_REMOTE_PASS environment variable is not set.")
	}

	port := os.Getenv("MOBLIN_REMOTE_PORT")
	if port == "" {
		port = "8080"
	}

	// Track event-status firing separately from synchronous GetStatus
	var eventStatusCount int
	var eventStatusMu sync.Mutex

	client := moblin.Register(mux, "/ws", pass, moblin.Callbacks{
		OnConnected: func() {
			fmt.Println("\n[Event] Moblin connected")
			fmt.Print("> ")
		},
		OnDisconnected: func() {
			fmt.Println("\n[Event] Moblin disconnected")
			fmt.Print("> ")
		},
		OnPing: func() {
			// Suppress ping noise unless raw logging
			if rawLog {
				fmt.Print("\n[Event] Ping\n> ")
			}
		},
		OnLog: func(entry string) {
			fmt.Printf("\n[Event] Log: %s\n> ", entry)
		},
		OnState: func(s moblin.StreamerState) {
			appState.Lock()
			defer appState.Unlock()

			if appState.LastState == nil {
				appState.LastState = &s
				fmt.Println("\n[Event] Initial state received")
				fmt.Print("> ")
				return
			}

			// Merge deltas
			if s.Streaming != nil {
				appState.LastState.Streaming = s.Streaming
			}
			if s.Recording != nil {
				appState.LastState.Recording = s.Recording
			}
			if s.Muted != nil {
				appState.LastState.Muted = s.Muted
			}
			if s.Zoom != nil {
				appState.LastState.Zoom = s.Zoom
			}
			if s.Scene != nil {
				appState.LastState.Scene = s.Scene
			}
			if s.Mic != nil {
				appState.LastState.Mic = s.Mic
			}
			if s.BatteryCharging != nil {
				appState.LastState.BatteryCharging = s.BatteryCharging
			}
			if s.TorchOn != nil {
				appState.LastState.TorchOn = s.TorchOn
			}
			if s.DebugLogging != nil {
				appState.LastState.DebugLogging = s.DebugLogging
			}
			if s.Bitrate != nil {
				appState.LastState.Bitrate = s.Bitrate
			}
			if s.ZoomPreset != nil {
				appState.LastState.ZoomPreset = s.ZoomPreset
			}
			if s.ZoomPresets != nil {
				appState.LastState.ZoomPresets = s.ZoomPresets
			}
			if s.AutoSceneSwitcher != nil {
				appState.LastState.AutoSceneSwitcher = s.AutoSceneSwitcher
			}
			if s.Filters != nil {
				appState.LastState.Filters = s.Filters
			}
		},
		OnStatus: func(s moblin.StatusPayload) {
			eventStatusMu.Lock()
			eventStatusCount++
			count := eventStatusCount
			eventStatusMu.Unlock()

			appState.Lock()
			appState.LastStatus = &s
			appState.Unlock()

			// Print periodically so we know it's firing
			if count == 1 || count%10 == 0 {
				fmt.Printf("\n[Event] Status push #%d received\n> ", count)
			}
		},
		OnScoreboard: func(config moblin.ScoreboardMatchConfig) {
			fmt.Printf("\n[Event] Scoreboard: %s vs %s (%s)\n> ",
				config.Team1.Name, config.Team2.Name, config.Global.Timer)
		},
		OnPreview: func(data []byte) {
			fmt.Printf("\n[Event] Preview frame: %d bytes\n> ", len(data))
		},
	})

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	fmt.Printf("Moblin Assistant CLI. Server listening on :%s/ws.\n", port)
	fmt.Println("Type 'help' for commands or 'quit' to exit.")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		cmd := strings.ToLower(parts[0])
		args := parts[1:]

		switch cmd {
		case "help":
			printHelp()

		// ── Existing commands ─────────────────────────────────────

		case "record":
			if len(args) != 1 || (args[0] != "on" && args[0] != "off") {
				fmt.Println("Usage: record [on|off]")
				continue
			}
			on := args[0] == "on"
			_, err := client.SendCommand(moblin.Request{
				SetRecord: &moblin.BoolReq{On: on},
			}, 3*time.Second)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("Recording set to %t\n", on)
			}

		case "zoom":
			if len(args) != 1 {
				fmt.Println("Usage: zoom <level>")
				continue
			}
			val, err := strconv.ParseFloat(args[0], 32)
			if err != nil {
				fmt.Printf("Error: invalid zoom level '%s'\n", args[0])
				continue
			}
			_, err = client.SendCommand(moblin.Request{
				SetZoom: &moblin.FloatReq{X: float32(val)},
			}, 3*time.Second)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("Zoom set to %.2f\n", val)
			}

		// ── Telemetry: synchronous GetStatus vs event-driven ─────

		case "telemetry":
			fmt.Println("--- Synchronous GetStatus ---")
			resp, err := client.SendCommand(moblin.Request{
				GetStatus: &struct{}{},
			}, 3*time.Second)
			if err != nil {
				fmt.Printf("Error fetching status: %v\n", err)
			} else if resp != nil && resp.GetStatus != nil && resp.GetStatus.General != nil {
				g := resp.GetStatus.General
				fmt.Printf("  Battery:   %d%%\n", safeInt(g.BatteryLevel))
				fmt.Printf("  Thermal:   %s\n", safeString(g.Flame))
				fmt.Printf("  Live:      %t\n", safeBool(g.IsLive))
				fmt.Printf("  Recording: %t\n", safeBool(g.IsRecording))
				fmt.Printf("  Muted:     %t\n", safeBool(g.IsMuted))
			} else {
				fmt.Println("  No general data in response.")
			}

			fmt.Println("--- Cached State (from events) ---")
			appState.RLock()
			if appState.LastState != nil {
				s := appState.LastState
				fmt.Printf("  Streaming: %t\n", safeBool(s.Streaming))
				fmt.Printf("  Recording: %t\n", safeBool(s.Recording))
				fmt.Printf("  Muted:     %t\n", safeBool(s.Muted))
				fmt.Printf("  Zoom:      %.2fx\n", safeFloat(s.Zoom))
				fmt.Printf("  Scene:     %s\n", safeString(s.Scene))
				fmt.Printf("  Mic:       %s\n", safeString(s.Mic))
				if s.Filters != nil {
					fmt.Printf("  Filters:   %v\n", s.Filters)
				}
			} else {
				fmt.Println("  No state events received yet.")
			}
			appState.RUnlock()

			fmt.Println("--- Cached Status (from event push) ---")
			eventStatusMu.Lock()
			count := eventStatusCount
			eventStatusMu.Unlock()
			appState.RLock()
			if appState.LastStatus != nil && count > 0 {
				fmt.Printf("  (received %d event-driven status pushes)\n", count)
				if appState.LastStatus.General != nil {
					fmt.Printf("  Battery:   %d%%\n", safeInt(appState.LastStatus.General.BatteryLevel))
				}
			} else {
				fmt.Println("  No event-driven status received yet.")
				fmt.Println("  (Use 'startstatus' to begin)")
			}
			appState.RUnlock()

		// ── StartStatus / StopStatus ─────────────────────────────

		case "startstatus":
			interval := 1
			if len(args) >= 1 {
				if v, err := strconv.Atoi(args[0]); err == nil {
					interval = v
				}
			}
			fmt.Printf("Requesting status push every %d second(s)...\n", interval)
			_, err := client.SendCommand(moblin.Request{
				StartStatus: &moblin.StartStatusReq{
					Interval: interval,
					Filter:   moblin.StartStatusFilter{TopRight: true},
				},
			}, 3*time.Second)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("Status push started.")
			}

		case "stopstatus":
			_, err := client.SendCommand(moblin.Request{
				StopStatus: &struct{}{},
			}, 3*time.Second)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("Status push stopped.")
			}

		// ── Stream toggle ────────────────────────────────────────

		case "stream":
			if len(args) != 1 || (args[0] != "on" && args[0] != "off") {
				fmt.Println("Usage: stream [on|off]")
				continue
			}
			on := args[0] == "on"
			_, err := client.SendCommand(moblin.Request{
				SetStream: &moblin.BoolReq{On: on},
			}, 3*time.Second)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("Streaming set to %t\n", on)
			}

		// ── Mute toggle ──────────────────────────────────────────

		case "mute":
			if len(args) != 1 || (args[0] != "on" && args[0] != "off") {
				fmt.Println("Usage: mute [on|off]")
				continue
			}
			on := args[0] == "on"
			_, err := client.SendCommand(moblin.Request{
				SetMute: &moblin.BoolReq{On: on},
			}, 3*time.Second)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("Mute set to %t\n", on)
			}

		// ── Torch toggle ─────────────────────────────────────────

		case "torch":
			if len(args) != 1 || (args[0] != "on" && args[0] != "off") {
				fmt.Println("Usage: torch [on|off]")
				continue
			}
			on := args[0] == "on"
			_, err := client.SendCommand(moblin.Request{
				SetTorch: &moblin.BoolReq{On: on},
			}, 3*time.Second)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("Torch set to %t\n", on)
			}

		// ── GetSettings ──────────────────────────────────────────

		case "settings":
			fmt.Println("Fetching settings...")
			resp, err := client.SendCommand(moblin.Request{
				GetSettings: &struct{}{},
			}, 3*time.Second)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}
			if resp == nil || resp.GetSettings == nil {
				fmt.Println("No settings data in response.")
				continue
			}
			s := resp.GetSettings.Data
			fmt.Println("Scenes:")
			for _, sc := range s.Scenes {
				fmt.Printf("  %s  %s\n", sc.ID, sc.Name)
			}
			fmt.Println("Mics:")
			for _, m := range s.Mics {
				fmt.Printf("  %s  %s\n", m.ID, m.Name)
			}
			fmt.Println("Bitrate Presets:")
			for _, b := range s.BitratePresets {
				fmt.Printf("  %s  %d bps\n", b.ID, b.Bitrate)
			}
			fmt.Println("SRT Connection Priorities (enabled:", s.Srt.ConnectionPrioritiesEnabled, "):")
			for _, p := range s.Srt.ConnectionPriorities {
				fmt.Printf("  %s  %s  pri=%d  enabled=%t\n", p.ID, p.Name, p.Priority, p.Enabled)
			}

		// ── Scene switch ─────────────────────────────────────────

		case "scene":
			if len(args) != 1 {
				fmt.Println("Usage: scene <uuid>")
				fmt.Println("  (use 'settings' to list available scene UUIDs)")
				continue
			}
			_, err := client.SendCommand(moblin.Request{
				SetScene: &moblin.UUIDReq{ID: args[0]},
			}, 3*time.Second)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("Scene set to %s\n", args[0])
			}

		// ── Mic switch ───────────────────────────────────────────

		case "mic":
			if len(args) < 1 {
				fmt.Println("Usage: mic <id>")
				fmt.Println("  (use 'settings' to list available mic IDs)")
				continue
			}
			id := strings.Join(args, " ")
			_, err := client.SendCommand(moblin.Request{
				SetMic: &moblin.StringIDReq{ID: id},
			}, 3*time.Second)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("Mic set to %s\n", id)
			}

		// ── Preview start/stop ───────────────────────────────────

		case "preview":
			if len(args) != 1 || (args[0] != "on" && args[0] != "off") {
				fmt.Println("Usage: preview [on|off]")
				continue
			}
			if args[0] == "on" {
				_, err := client.SendCommand(moblin.Request{
					StartPreview: &struct{}{},
				}, 3*time.Second)
				if err != nil {
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Println("Preview started. Watch for '[Event] Preview frame' messages.")
				}
			} else {
				_, err := client.SendCommand(moblin.Request{
					StopPreview: &struct{}{},
				}, 3*time.Second)
				if err != nil {
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Println("Preview stopped.")
				}
			}

		// ── Filter toggle ────────────────────────────────────────

		case "filter":
			if len(args) != 2 || (args[1] != "on" && args[1] != "off") {
				fmt.Println("Usage: filter <name> [on|off]")
				fmt.Println("  Names: pixellate, movie, grayScale, sepia, triple, twin,")
				fmt.Println("         fourThree, pinch, whirlpool, poll, blurFaces,")
				fmt.Println("         privacy, beauty, moblinInMouth, cameraMan")
				continue
			}
			on := args[1] == "on"
			_, err := client.SendCommand(moblin.Request{
				SetFilter: &moblin.SetFilterReq{Filter: moblin.SwiftEnum{Name: args[0]}, On: on},
			}, 3*time.Second)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("Filter %s set to %t\n", args[0], on)
			}

		// ── Scoreboard commands ──────────────────────────────────

		case "scoreboard":
			if len(args) < 1 {
				fmt.Println("Usage:")
				fmt.Println("  scoreboard sports          - List available sports")
				fmt.Println("  scoreboard set <sportId>   - Set sport type")
				fmt.Println("  scoreboard clock           - Toggle clock")
				fmt.Println("  scoreboard update          - Send test scoreboard update")
				continue
			}
			switch args[0] {
			case "sports":
				resp, err := client.SendCommand(moblin.Request{
					GetScoreboardSports: &struct{}{},
				}, 3*time.Second)
				if err != nil {
					fmt.Printf("Error: %v\n", err)
				} else if resp != nil && resp.GetScoreboardSports != nil {
					fmt.Println("Available sports:")
					for _, name := range resp.GetScoreboardSports.Names {
						fmt.Printf("  %s\n", name)
					}
				} else {
					fmt.Println("No scoreboard sports data in response.")
				}
			case "set":
				if len(args) < 2 {
					fmt.Println("Usage: scoreboard set <sportId>")
					continue
				}
				_, err := client.SendCommand(moblin.Request{
					SetScoreboardSport: &moblin.SportIDReq{SportID: args[1]},
				}, 3*time.Second)
				if err != nil {
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Printf("Scoreboard sport set to %s\n", args[1])
				}
			case "clock":
				_, err := client.SendCommand(moblin.Request{
					ToggleScoreboardClock: &struct{}{},
				}, 3*time.Second)
				if err != nil {
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Println("Scoreboard clock toggled.")
				}
			case "update":
				_, err := client.SendCommand(moblin.Request{
					UpdateScoreboard: &moblin.UpdateScoreboardReq{
						Config: moblin.ScoreboardMatchConfig{
							SportID: "volleyball",
							Layout:  "default",
							Team1: moblin.ScoreboardTeam{
								Name:         "Home",
								BgColor:      "#0000FF",
								TextColor:    "#FFFFFF",
								PrimaryScore: "25",
							},
							Team2: moblin.ScoreboardTeam{
								Name:         "Away",
								BgColor:      "#FF0000",
								TextColor:    "#FFFFFF",
								PrimaryScore: "23",
							},
							Global: moblin.GlobalStats{
								Title:          "Set 3",
								Timer:          "00:00",
								TimerDirection: "up",
								Period:         "3",
								PeriodLabel:    "Set",
							},
							Controls: map[string]moblin.ScoreboardControl{},
						},
					},
				}, 3*time.Second)
				if err != nil {
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Println("Scoreboard updated with test volleyball data.")
				}
			default:
				fmt.Printf("Unknown scoreboard subcommand: %s\n", args[0])
			}

		// ── Raw JSON logging ─────────────────────────────────────

		case "rawlog":
			rawLog = !rawLog
			if rawLog {
				fmt.Println("Raw JSON logging ENABLED.")
			} else {
				fmt.Println("Raw JSON logging DISABLED.")
			}

		// ── Raw status dump ──────────────────────────────────────

		case "rawstatus":
			resp, err := client.SendCommand(moblin.Request{
				GetStatus: &struct{}{},
			}, 3*time.Second)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}
			raw, _ := json.MarshalIndent(resp, "  ", "  ")
			fmt.Println("Parsed ResponseData re-serialized:")
			fmt.Println(string(raw))

		// ── Quit ─────────────────────────────────────────────────

		case "quit", "exit":
			fmt.Println("Shutting down...")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := srv.Shutdown(ctx); err != nil {
				log.Fatalf("Server forced to shutdown: %v", err)
			}
			fmt.Println("Exited cleanly.")
			return

		default:
			fmt.Printf("Unknown command: %s (type 'help')\n", cmd)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Scanner error: %v", err)
	}
}

func printHelp() {
	fmt.Println(`
Commands:
  help              Show this message

  ── Telemetry & Status ──
  telemetry         Show synced state + synchronous GetStatus + event status
  startstatus [n]   Start event-driven status push (every n seconds, default 1)
  stopstatus        Stop event-driven status push
  rawstatus         GetStatus and dump the re-serialized JSON for inspection
  rawlog            Toggle raw JSON logging for all inbound messages

  ── Stream Controls ──
  record [on|off]   Toggle recording
  stream [on|off]   Toggle streaming
  mute [on|off]     Toggle mute
  torch [on|off]    Toggle flashlight
  zoom <float>      Set zoom level
  preview [on|off]  Start/stop JPEG preview frames

  ── Configuration ──
  settings          Dump scenes, mics, bitrate presets, SRT priorities
  scene <uuid>      Switch scene (get UUIDs from 'settings')
  mic <id>          Switch mic (get IDs from 'settings')

  ── Filters ──
  filter <n> [on|off]   Toggle a video filter

  ── Scoreboard (volleyball!) ──
  scoreboard sports         List available sports
  scoreboard set <sportId>  Set active sport
  scoreboard clock          Toggle clock
  scoreboard update         Push test volleyball scoreboard

  quit / exit       Shutdown`)
}

func safeBool(b *bool) bool {
	if b == nil {
		return false
	}
	return *b
}

func safeFloat(f *float32) float32 {
	if f == nil {
		return 0.0
	}
	return *f
}

func safeInt(i *int) int {
	if i == nil {
		return 0
	}
	return *i
}

func safeString(s *string) string {
	if s == nil {
		return "N/A"
	}
	return *s
}
