package moblin

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
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

	// OnTwitchStart is triggered when the streamer requests the assistant to proxy
	// Twitch EventSub notifications and chat. The access token is AES-GCM encrypted
	// with the shared password and base64-encoded. Decryption is the caller's responsibility.
	OnTwitchStart func(data TwitchStartData)

	// OnRawMessage is triggered for every WebSocket message received, before any JSON
	// unmarshalling or protocol handling occurs. The payload is the raw UTF-8 bytes
	// exactly as read off the wire. This is intended for diagnostics and protocol
	// debugging — it fires for all message types including identify, ping, event,
	// response, and preview frames. Avoid expensive work in this callback as it
	// runs synchronously in the read loop and will block all message processing.
	OnRawMessage func(payload []byte)
}

type WSConn struct {
	conn       *websocket.Conn
	writeMutex sync.Mutex
}

// Client represents a connected Moblin device.
type Client struct {
	wsConn    *WSConn
	connMutex sync.RWMutex
	password  string
	callbacks Callbacks

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
	wsConn := &WSConn{
		conn: conn,
	}
	wsConn.conn.SetReadLimit(10 * 1024 * 1024) // 10mb
	wsConn.conn.SetReadDeadline(time.Now().Add(60 * time.Second))

	defer func() {
		c.connMutex.Lock()
		if c.wsConn != wsConn {
			c.connMutex.Unlock()
			return
		}
		c.wsConn = nil
		c.connMutex.Unlock()

		c.drainPending()

		if c.callbacks.OnDisconnected != nil {
			c.callbacks.OnDisconnected()
		}
	}()

	defer conn.Close()

	// 1. Generate Challenge and Salt
	challenge, err := generateRandomString(16)
	if err != nil {
		conn.Close()
		return
	}
	salt, err := generateRandomString(16)
	if err != nil {
		conn.Close()
		return
	}

	// 2. Send Hello
	err = c.sendJSON(wsConn, MessageToStreamer{
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

	c.readLoop(wsConn, challenge, salt)
}

func (c *Client) readLoop(wsConn *WSConn, challenge, salt string) {
	identified := false

	for {
		wsConn.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		_, payload, err := wsConn.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				log.Printf("WebSocket read error: %v\n", err)
			}
			break
		}

		if c.callbacks.OnRawMessage != nil {
			c.callbacks.OnRawMessage(payload)
		}

		var msg MessageToAssistant
		if err := json.Unmarshal(payload, &msg); err != nil {
			log.Printf("JSON unmarshal error: %v\n", err)
			if !identified {
				// Abort connection on garbage data during auth phase
				return
			}
			continue
		}

		// Handle Authentication Phase
		if !identified {
			if msg.Identify == nil {
				// Disconnect immediately on invalid initial payload
				return
			}
			expectedHash := hashPassword(challenge, salt, c.password)
			if subtle.ConstantTimeCompare([]byte(msg.Identify.Authentication), []byte(expectedHash)) == 1 {
				identified = true

				c.connMutex.Lock()
				var closeConn *WSConn
				if c.wsConn != nil {
					closeConn = c.wsConn
				}
				c.wsConn = wsConn
				c.connMutex.Unlock()

				c.drainPending()

				if closeConn != nil {
					closeConn.conn.Close()
					if c.callbacks.OnDisconnected != nil {
						c.callbacks.OnDisconnected()
					}
				}

				c.sendJSON(wsConn, MessageToStreamer{Identified: &IdentifiedData{
					Result: ResultPayload{Ok: &struct{}{}},
				}})
				if c.callbacks.OnConnected != nil {
					c.callbacks.OnConnected()
				}
				continue
			}

			c.sendJSON(wsConn, MessageToStreamer{Identified: &IdentifiedData{
				Result: ResultPayload{WrongPassword: &struct{}{}},
			}})
			return
		}

		// Handle Telemetry & Responses Phase
		c.processMessage(wsConn, msg)
	}
}

// drainPending closes all pending response channels, unblocking any callers
// waiting in SendCommand. Must be called when a connection is no longer viable.
func (c *Client) drainPending() {
	c.pending.Range(func(key, value interface{}) bool {
		if ch, ok := value.(chan Response); ok {
			close(ch)
		}
		c.pending.Delete(key)
		return true
	})
}

func (c *Client) processMessage(wsConn *WSConn, msg MessageToAssistant) {
	if msg.Ping != nil {
		c.sendJSON(wsConn, MessageToStreamer{Pong: &struct{}{}})
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
		} else if payload.Scoreboard != nil && c.callbacks.OnScoreboard != nil {
			c.callbacks.OnScoreboard(payload.Scoreboard.Config)
		}
		return
	}

	if msg.TwitchStart != nil && c.callbacks.OnTwitchStart != nil {
		c.callbacks.OnTwitchStart(*msg.TwitchStart)
		return
	}

	if msg.Response != nil {
		if val, ok := c.pending.Load(msg.Response.ID); ok {
			if ch, valid := val.(chan Response); valid {
				select {
				case ch <- *msg.Response:
				default:
				}
			}
			c.pending.Delete(msg.Response.ID)
		}
		return
	}

	if msg.Preview != nil && c.callbacks.OnPreview != nil {
		c.callbacks.OnPreview(msg.Preview.Preview)
	}
}

// sendJSON safely writes JSON to the WebSocket.
func (c *Client) sendJSON(wsConn *WSConn, v interface{}) error {
	wsConn.writeMutex.Lock()
	defer wsConn.writeMutex.Unlock()
	wsConn.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return wsConn.conn.WriteJSON(v)
}

// SendCommand executes a synchronous request to the Moblin app.
func (c *Client) SendCommand(req Request, timeout time.Duration) (*ResponseData, error) {
	c.connMutex.RLock()
	wsConn := c.wsConn
	c.connMutex.RUnlock()
	if wsConn == nil {
		return nil, fmt.Errorf("No connection to send to")
	}
	id := atomic.AddUint64(&c.requestID, 1)
	respChan := make(chan Response, 1)
	c.pending.Store(id, respChan)
	defer c.pending.Delete(id)

	err := c.sendJSON(wsConn, MessageToStreamer{
		Request: &RequestPayload{
			ID:   id,
			Data: req,
		},
	})
	if err != nil {
		return nil, err
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case resp, ok := <-respChan:
		if !ok {
			return nil, errors.New("connection lost")
		}
		if resp.Result.Ok == nil {
			return nil, errors.New("command failed: non-ok result received")
		}
		return resp.Data, nil
	case <-timer.C:
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

func generateRandomString(n int) (string, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(b), nil
}
