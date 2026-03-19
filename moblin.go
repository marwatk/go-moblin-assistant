package moblin

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true }, // Adjust for production
}

// Callbacks defines the event hooks for handling asynchronous data pushed by the Moblin app.
// These are strictly for unidirectional, streamer-to-assistant events, not synchronous RPC responses.
type Callbacks struct {
	// OnConnected is triggered when the WebSocket connection is fully established
	// AND the double-SHA256 authentication challenge is successfully validated.
	OnConnected func()

	// OnDisconnected is triggered when the WebSocket drops. This occurs on raw TCP
	// disconnections, WebSocket close frames, or if the internal ping/pong keepalive timer times out.
	OnDisconnected func()

	// OnPing is triggered when the assistant receives a `{"ping": {}}` payload.
	// The Moblin app sends this periodically (typically every 30 seconds) to keep the connection alive.
	// The underlying library automatically replies with a Pong; this hook is for external observability.
	OnPing func()

	// OnLog is triggered when the Moblin app pushes an internal debug or lifecycle log.
	// Examples include audio/video encoder initializations, file writer creations, and system errors.
	OnLog func(entry string)

	// OnState is triggered ONLY when a StreamerState property mutates (e.g., recording toggled,
	// zoom changed, filter applied).
	// IMPORTANT: Moblin sends DELTAS (partial updates), not the full state. You must merge
	// this incoming payload with your locally cached state; otherwise, omitted fields will be overwritten as nil.
	OnState func(state StreamerState)

	// OnStatus is triggered continuously at an interval specified by a prior `StartStatus` request.
	// It provides real-time device metrics (battery level, thermal flame state, WiFi SSID)
	// and stream metrics (audio levels, current bitrate).
	// If `StartStatus` has not been sent, this callback will never fire.
	OnStatus func(status StatusPayload)

	// OnScoreboard is triggered when the stream's scoreboard mutates.
	// This includes clock ticks, score increments, period changes, and possession toggles.
	OnScoreboard func(config ScoreboardMatchConfig)

	// OnPreview is triggered when the app pushes a raw video frame (typically JPEG).
	// This requires a prior `StartPreview` request to initiate the continuous frame stream.
	OnPreview func(previewData []byte)
}

// Client represents a connected Moblin device.
type Client struct {
	conn       *websocket.Conn
	writeMutex sync.Mutex
	password   string
	callbacks  Callbacks

	requestID uint64
	pending   sync.Map // map[uint64]chan Response
}

// Register registers the Moblin endpoint with the provided ServeMux.
func Register(mux *http.ServeMux, path string, password string, callbacks Callbacks) *Client {
	c := &Client{
		password:  password,
		callbacks: callbacks,
	}

	mux.HandleFunc(path, c.handleWebSocket)
	return c
}

func (c *Client) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return // Connection failed
	}
	c.conn = conn
	defer c.conn.Close()
	defer func() {
		if c.callbacks.OnDisconnected != nil {
			c.callbacks.OnDisconnected()
		}
	}()

	// 1. Generate Challenge and Salt
	challenge := generateRandomString(16)
	salt := generateRandomString(16)

	// 2. Send Hello
	err = c.sendJSON(MessageToStreamer{
		Hello: &HelloData{
			ApiVersion: "0.1",
			Authentication: Authentication{
				Challenge: challenge,
				Salt:      salt,
			},
		},
	})
	if err != nil {
		return
	}

	c.readLoop(challenge, salt)
}

func (c *Client) readLoop(challenge, salt string) {
	identified := false

	for {
		_, payload, err := c.conn.ReadMessage()
		if err != nil {
			log.Printf("WebSocket read error: %v\n", err)
			break
		}

		var msg MessageToAssistant
		if err := json.Unmarshal(payload, &msg); err != nil {
			log.Printf("JSON unmarshal error: %v\n", err)
			continue
		}

		// Handle Authentication Phase
		if !identified {
			if msg.Identify != nil {
				expectedHash := hashPassword(challenge, salt, c.password)
				if msg.Identify.Authentication == expectedHash {
					identified = true
					c.sendJSON(MessageToStreamer{Identified: &IdentifiedData{
						Result: ResultPayload{Ok: &struct{}{}},
					}})
					if c.callbacks.OnConnected != nil {
						c.callbacks.OnConnected()
					}
					continue
				}

				c.sendJSON(MessageToStreamer{Identified: &IdentifiedData{
					Result: ResultPayload{WrongPassword: &struct{}{}},
				}})
				return
			}
			continue
		}

		// Handle Telemetry & Responses Phase
		c.processMessage(msg)
	}
}

func (c *Client) processMessage(msg MessageToAssistant) {
	if msg.Ping != nil {
		c.sendJSON(MessageToStreamer{Pong: &struct{}{}})
		if c.callbacks.OnPing != nil {
			c.callbacks.OnPing()
		}
		return
	}

	if msg.Event != nil {
		payload := msg.Event.Data // Unwrap the nested "data" struct

		if payload.Log != nil && c.callbacks.OnLog != nil {
			c.callbacks.OnLog(payload.Log.Entry)
		} else if payload.State != nil && c.callbacks.OnState != nil {
			c.callbacks.OnState(payload.State.Data)
		} else if payload.Status != nil && c.callbacks.OnStatus != nil {
			c.callbacks.OnStatus(payload.Status.Data)
		}
		return
	}

	if msg.Response != nil {
		if ch, ok := c.pending.Load(msg.Response.ID); ok {
			// Corrected: Use the channel send operator
			ch.(chan Response) <- *msg.Response
			c.pending.Delete(msg.Response.ID)
		}
		return
	}

	if msg.Preview != nil && c.callbacks.OnPreview != nil {
		c.callbacks.OnPreview(msg.Preview.Preview)
	}
}

// sendJSON safely writes JSON to the WebSocket.
func (c *Client) sendJSON(v interface{}) error {
	c.writeMutex.Lock()
	defer c.writeMutex.Unlock()
	return c.conn.WriteJSON(v)
}

// SendCommand executes a synchronous request to the Moblin app.
func (c *Client) SendCommand(req Request, timeout time.Duration) (*ResponseData, error) {
	id := atomic.AddUint64(&c.requestID, 1)
	respChan := make(chan Response, 1)
	c.pending.Store(id, respChan)
	defer c.pending.Delete(id)

	err := c.sendJSON(MessageToStreamer{
		Request: &RequestPayload{
			ID:   id,
			Data: req,
		},
	})
	if err != nil {
		return nil, err
	}

	select {
	case resp := <-respChan:
		if resp.Result.Ok == nil {
			return nil, fmt.Errorf("command failed: non-ok result received")
		}
		return resp.Data, nil
	case <-time.After(timeout):
		return nil, errors.New("timeout waiting for response")
	}
}

// Helpers

func hashPassword(challenge, salt, password string) string {
	s1 := password + salt
	h1 := sha256.Sum256([]byte(s1))
	b64_1 := base64.StdEncoding.EncodeToString(h1[:])

	s2 := b64_1 + challenge
	h2 := sha256.Sum256([]byte(s2))
	return base64.StdEncoding.EncodeToString(h2[:])
}

func generateRandomString(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return base64.URLEncoding.EncodeToString(b)
}
