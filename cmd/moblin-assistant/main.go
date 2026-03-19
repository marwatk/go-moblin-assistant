package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/marwatk/go-moblin-assistant"
)

// AppState holds the latest telemetry data received from the app.
// It must be protected by a mutex as it is accessed across goroutines.
type AppState struct {
	sync.RWMutex
	LastState  *moblin.StreamerState
	LastStatus *moblin.StatusPayload
}

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

	// Initialize the Moblin endpoint with callbacks that write to AppState
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
			fmt.Print("\n[Event] Ping received\n> ")
		},
		OnState: func(s moblin.StreamerState) {
			appState.Lock()
			defer appState.Unlock()

			// If we have no cached state, accept the first payload completely
			if appState.LastState == nil {
				appState.LastState = &s
				return
			}

			// Otherwise, merge only the fields that were included in the JSON delta
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
		},
		OnStatus: func(s moblin.StatusPayload) {
			appState.Lock()
			appState.LastStatus = &s
			appState.Unlock()
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
			fmt.Println("Available commands:")
			fmt.Println("  help      - Show this message")
			fmt.Println("  record    - Toggle recording (usage: record [on|off])")
			fmt.Println("  telemetry - Fetch and show latest state and status")
			fmt.Println("  zoom      - Set camera zoom level (usage: zoom <float>)")
			fmt.Println("  quit      - Shut down server and exit")

		case "record":
			if len(args) != 1 || (args[0] != "on" && args[0] != "off") {
				fmt.Println("Usage: record [on|off]")
				continue
			}
			on := args[0] == "on"

			// Execute synchronous request
			_, err := client.SendCommand(moblin.Request{
				SetRecord: &moblin.BoolReq{On: on},
			}, 3*time.Second)

			if err != nil {
				fmt.Printf("Error: failed to set recording to %t: %v\n", on, err)
			} else {
				fmt.Printf("Successfully set recording to %t\n", on)
			}

		case "telemetry":
			fmt.Println("Fetching latest telemetry...")

			// 1. Fetch Status synchronously
			resp, err := client.SendCommand(moblin.Request{
				GetStatus: &struct{}{},
			}, 3*time.Second)

			if err != nil {
				fmt.Printf("Error fetching status: %v\n", err)
				continue
			}

			fmt.Println("--- Latest Telemetry ---")

			// 2. Read State from event-driven cache
			appState.RLock()
			if appState.LastState != nil {
				fmt.Println("State (Cached from events):")
				fmt.Printf("  Streaming: %t\n", safeBool(appState.LastState.Streaming))
				fmt.Printf("  Recording: %t\n", safeBool(appState.LastState.Recording))
				fmt.Printf("  Muted:     %t\n", safeBool(appState.LastState.Muted))
				fmt.Printf("  Zoom:      %.2fx\n", safeFloat(appState.LastState.Zoom))
			} else {
				fmt.Println("State: No event data received yet.")
			}
			appState.RUnlock()

			// 3. Output fetched Status
			if resp != nil && resp.GetStatus != nil && resp.GetStatus.General != nil {
				fmt.Println("Status (Fetched directly):")
				fmt.Printf("  Battery:   %d%%\n", safeInt(resp.GetStatus.General.BatteryLevel))
				fmt.Printf("  Thermal:   %s\n", safeString(resp.GetStatus.General.Flame))
				fmt.Printf("  Live:      %t\n", safeBool(resp.GetStatus.General.IsLive))
				fmt.Printf("  Recording: %t\n", safeBool(resp.GetStatus.General.IsRecording))
			} else {
				fmt.Println("Status: No general data in response.")
			}
		case "zoom":
			if len(args) != 1 {
				fmt.Println("Usage: zoom <level>")
				continue
			}

			val, err := strconv.ParseFloat(args[0], 32)
			if err != nil {
				fmt.Printf("Error: invalid zoom level '%s'. Must be a float.\n", args[0])
				continue
			}

			_, err = client.SendCommand(moblin.Request{
				SetZoom: &moblin.FloatReq{X: float32(val)},
			}, 3*time.Second)

			if err != nil {
				fmt.Printf("Error: failed to set zoom to %.2f: %v\n", val, err)
			} else {
				fmt.Printf("Successfully set zoom to %.2f\n", val)
			}
		case "quit", "exit":
			fmt.Println("Initiating graceful shutdown...")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if err := srv.Shutdown(ctx); err != nil {
				log.Fatalf("Server forced to shutdown: %v", err)
			}
			fmt.Println("Exited cleanly.")
			return

		default:
			fmt.Printf("Unknown command: %s\n", cmd)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Scanner error: %v", err)
	}
}

// Dereferencing helpers for pointer fields
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
		return "Unknown"
	}
	return *s
}
