package okx

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/tigusigalpa/okx-go/models"
)

const (
	// WebSocket URLs
	WSPublicURL        = "wss://ws.okx.com:8443/ws/v5/public"
	WSPrivateURL       = "wss://ws.okx.com:8443/ws/v5/private"
	WSBusinessURL      = "wss://ws.okx.com:8443/ws/v5/business"
	WSPublicSBEURL     = "wss://ws.okx.com:8443/ws/v5/public-sbe"
	WSDemoPublicURL    = "wss://wspap.okx.com:8443/ws/v5/public"
	WSDemoPrivateURL   = "wss://wspap.okx.com:8443/ws/v5/private"
	WSDemoBusinessURL  = "wss://wspap.okx.com:8443/ws/v5/business"
	WSDemoPublicSBEURL = "wss://wspap.okx.com:8443/ws/v5/public-sbe"

	pingInterval = 25 * time.Second
	pongTimeout  = 30 * time.Second
	writeTimeout = 10 * time.Second
	readTimeout  = 60 * time.Second
)

type WSClient struct {
	apiKey     string
	secretKey  string
	passphrase string
	url        string
	conn       *websocket.Conn
	isDemo     bool
	logger     Logger

	mu             sync.RWMutex
	subscriptions  map[string]*wsSubscription
	reconnectCbs   []func()
	loginResult    chan error
	done           chan struct{}
	reconnect      bool
	loginRequested bool
	authenticated  bool
	closed         bool
}

type wsSubscription struct {
	channel string
	args    map[string]interface{}
	ch      chan []byte
	ack     chan error
}

type wsSubscriptionSnapshot struct {
	key     string
	channel string
	args    map[string]interface{}
}

type WSOption func(*WSClient)

func WithWSDemo() WSOption {
	return func(ws *WSClient) {
		ws.isDemo = true
	}
}

func WithWSLogger(logger Logger) WSOption {
	return func(ws *WSClient) {
		ws.logger = logger
	}
}

func NewWSClient(apiKey, secretKey, passphrase, url string, opts ...WSOption) *WSClient {
	ws := &WSClient{
		apiKey:        apiKey,
		secretKey:     secretKey,
		passphrase:    passphrase,
		url:           url,
		logger:        &noopLogger{},
		subscriptions: make(map[string]*wsSubscription),
		done:          make(chan struct{}),
		reconnect:     true,
	}

	for _, opt := range opts {
		opt(ws)
	}

	return ws
}

func (ws *WSClient) Connect(ctx context.Context) error {
	ws.mu.RLock()
	closed := ws.closed
	ws.mu.RUnlock()
	if closed {
		return errors.New("WebSocket client is closed")
	}

	dialer := websocket.DefaultDialer
	conn, _, err := dialer.DialContext(ctx, ws.url, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to WebSocket: %w", err)
	}

	ws.mu.Lock()
	if ws.closed {
		ws.mu.Unlock()
		_ = conn.Close()
		return errors.New("WebSocket client is closed")
	}
	ws.conn = conn
	ws.mu.Unlock()

	ws.logger.Info("WebSocket connected", "url", ws.url)

	go ws.readPump(conn)
	go ws.pingPump(conn)

	return nil
}

func (ws *WSClient) Login(ctx context.Context) error {
	result := make(chan error, 1)
	ws.mu.Lock()
	if ws.loginResult != nil {
		ws.mu.Unlock()
		return errors.New("WebSocket login already in progress")
	}
	ws.authenticated = false
	ws.loginRequested = true
	ws.loginResult = result
	ws.mu.Unlock()

	timestamp := fmt.Sprintf("%d", time.Now().Unix())
	message := timestamp + "GET" + "/users/self/verify"
	h := hmac.New(sha256.New, []byte(ws.secretKey))
	h.Write([]byte(message))
	sign := base64.StdEncoding.EncodeToString(h.Sum(nil))

	loginReq := models.WSLoginRequest{
		Op: "login",
		Args: []models.WSLoginArgs{
			{
				APIKey:     ws.apiKey,
				Passphrase: ws.passphrase,
				Timestamp:  timestamp,
				Sign:       sign,
			},
		},
	}

	if err := ws.send(loginReq); err != nil {
		ws.clearLoginResult(result)
		return fmt.Errorf("failed to send login request: %w", err)
	}

	select {
	case err := <-result:
		if err != nil {
			return err
		}
		ws.logger.Info("WebSocket authenticated")
		return nil
	case <-ctx.Done():
		ws.clearLoginResult(result)
		return fmt.Errorf("login confirmation timeout: %w", ctx.Err())
	}
}

func (ws *WSClient) subscribeArgs(channel string, args map[string]interface{}) map[string]interface{} {
	subArgs := make(map[string]interface{}, len(args)+1)
	subArgs["channel"] = channel
	for k, v := range args {
		subArgs[k] = v
	}
	return subArgs
}

func cloneWSArgs(args map[string]interface{}) map[string]interface{} {
	cloned := make(map[string]interface{}, len(args))
	for k, v := range args {
		cloned[k] = v
	}
	return cloned
}

func (ws *WSClient) Subscribe(ctx context.Context, channel string, args map[string]interface{}) (<-chan []byte, error) {
	subKey := ws.makeSubKey(channel, args)

	ws.mu.Lock()
	if _, exists := ws.subscriptions[subKey]; exists {
		ws.mu.Unlock()
		return nil, fmt.Errorf("already subscribed to %s", subKey)
	}

	ch := make(chan []byte, 100)
	ack := make(chan error, 1)
	ws.subscriptions[subKey] = &wsSubscription{
		channel: channel,
		args:    cloneWSArgs(args),
		ch:      ch,
		ack:     ack,
	}
	ws.mu.Unlock()

	subReq := models.WSSubscribeRequest{
		Op:   "subscribe",
		Args: []map[string]interface{}{ws.subscribeArgs(channel, args)},
	}

	if err := ws.send(subReq); err != nil {
		ws.removeSubscription(subKey)
		return nil, fmt.Errorf("failed to send subscribe request: %w", err)
	}

	if err := ws.waitSubscription(ctx, subKey, ack); err != nil {
		ws.removeSubscription(subKey)
		return nil, err
	}

	ws.logger.Info("Subscribed to channel", "channel", channel, "args", args)

	return ch, nil
}

func (ws *WSClient) Unsubscribe(channel string, args map[string]interface{}) error {
	subKey := ws.makeSubKey(channel, args)

	ws.mu.Lock()
	ch, exists := ws.subscriptions[subKey]
	if !exists {
		ws.mu.Unlock()
		return fmt.Errorf("not subscribed to %s", subKey)
	}
	delete(ws.subscriptions, subKey)
	close(ch.ch)
	ws.mu.Unlock()

	unsubReq := models.WSUnsubscribeRequest{
		Op:   "unsubscribe",
		Args: []map[string]interface{}{ws.subscribeArgs(channel, args)},
	}

	if err := ws.send(unsubReq); err != nil {
		return fmt.Errorf("failed to send unsubscribe request: %w", err)
	}

	ws.logger.Info("Unsubscribed from channel", "channel", channel, "args", args)

	return nil
}

func (ws *WSClient) waitSubscription(ctx context.Context, subKey string, ack chan error) error {
	select {
	case err := <-ack:
		if err != nil {
			return err
		}
		ws.clearSubscriptionAck(subKey, ack)
		return nil
	case <-ctx.Done():
		ws.clearSubscriptionAck(subKey, ack)
		return fmt.Errorf("subscription confirmation timeout for %s: %w", subKey, ctx.Err())
	}
}

func (ws *WSClient) removeSubscription(subKey string) {
	ws.mu.Lock()
	sub, exists := ws.subscriptions[subKey]
	if exists {
		delete(ws.subscriptions, subKey)
	}
	ws.mu.Unlock()
	if exists {
		close(sub.ch)
	}
}

func (ws *WSClient) clearSubscriptionAck(subKey string, ack chan error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	sub, exists := ws.subscriptions[subKey]
	if exists && sub.ack == ack {
		sub.ack = nil
	}
}

func (ws *WSClient) completeSubscription(channel string, arg map[string]interface{}, err error) {
	subKey := ws.makeSubKeyFromArg(channel, arg)
	ws.mu.Lock()
	sub, exists := ws.subscriptions[subKey]
	var ack chan error
	if exists {
		ack = sub.ack
		sub.ack = nil
	}
	ws.mu.Unlock()
	if !exists {
		ws.logger.Error("No subscription waiting for confirmation", "channel", channel, "arg", arg)
		return
	}
	if ack == nil {
		return
	}
	select {
	case ack <- err:
	default:
	}
}

func (ws *WSClient) clearLoginResult(result chan error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.loginResult == result {
		ws.loginResult = nil
	}
}

func (ws *WSClient) completeLogin(err error) {
	ws.mu.Lock()
	ws.authenticated = err == nil
	result := ws.loginResult
	ws.loginResult = nil
	ws.mu.Unlock()
	if result == nil {
		return
	}
	select {
	case result <- err:
	default:
	}
}

func (ws *WSClient) hasPendingLogin() bool {
	ws.mu.RLock()
	defer ws.mu.RUnlock()
	return ws.loginResult != nil
}

func (ws *WSClient) eventError(event, code, msg string) error {
	if code == "" && msg == "" {
		return fmt.Errorf("WebSocket %s failed", event)
	}
	return fmt.Errorf("WebSocket %s failed: code=%s msg=%s", event, code, msg)
}

func (ws *WSClient) SubscribeReconnect(cb func()) {
	if cb == nil {
		return
	}
	ws.mu.Lock()
	defer ws.mu.Unlock()
	ws.reconnectCbs = append(ws.reconnectCbs, cb)
}

func (ws *WSClient) Close() error {
	ws.mu.Lock()
	ws.reconnect = false
	if !ws.closed {
		close(ws.done)
		ws.closed = true
	}
	closeErr := errors.New("WebSocket closed")
	loginResult := ws.loginResult
	ws.loginResult = nil
	acks := make([]chan error, 0, len(ws.subscriptions))
	for _, sub := range ws.subscriptions {
		if sub.ack != nil {
			acks = append(acks, sub.ack)
			sub.ack = nil
		}
		close(sub.ch)
	}
	ws.subscriptions = make(map[string]*wsSubscription)
	conn := ws.conn
	ws.conn = nil
	ws.mu.Unlock()

	if loginResult != nil {
		select {
		case loginResult <- closeErr:
		default:
		}
	}
	for _, ack := range acks {
		select {
		case ack <- closeErr:
		default:
		}
	}
	if conn != nil {
		return conn.Close()
	}

	return nil
}

func (ws *WSClient) send(v interface{}) error {
	ws.mu.RLock()
	conn := ws.conn
	ws.mu.RUnlock()

	if conn == nil {
		return errors.New("WebSocket not connected")
	}

	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	return nil
}

func (ws *WSClient) readPump(conn *websocket.Conn) {
	for {
		select {
		case <-ws.done:
			return
		default:
		}

		if !ws.isCurrentConn(conn) {
			return
		}

		conn.SetReadDeadline(time.Now().Add(readTimeout))
		_, message, err := conn.ReadMessage()
		if err != nil {
			ws.logger.Error("WebSocket read error", "error", err)
			ws.closeConn(conn)
			if ws.reconnect {
				ws.handleReconnect()
			}
			return
		}

		if string(message) == "pong" {
			ws.logger.Debug("Received pong")
			continue
		}

		ws.handleMessage(message)
	}
}

func (ws *WSClient) pingPump(conn *websocket.Conn) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ws.done:
			return
		case <-ticker.C:
			if !ws.isCurrentConn(conn) {
				return
			}

			conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
				ws.logger.Error("WebSocket ping error", "error", err)
				ws.closeConn(conn)
				return
			}
			ws.logger.Debug("Sent ping")
		}
	}
}

func (ws *WSClient) isCurrentConn(conn *websocket.Conn) bool {
	ws.mu.RLock()
	defer ws.mu.RUnlock()
	return ws.conn == conn && conn != nil
}

func (ws *WSClient) closeConn(conn *websocket.Conn) {
	ws.mu.Lock()
	if ws.conn == conn {
		ws.conn = nil
	}
	ws.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func (ws *WSClient) handleMessage(message []byte) {
	var resp models.WSResponse
	if err := json.Unmarshal(message, &resp); err != nil {
		ws.logger.Error("Failed to unmarshal WebSocket message", "error", err, "message", string(message))
		return
	}

	if resp.Event == "error" {
		err := ws.eventError(resp.Event, resp.Code, resp.Msg)
		ws.logger.Error("WebSocket error event", "code", resp.Code, "msg", resp.Msg)
		if resp.Arg != nil {
			if channel, ok := resp.Arg["channel"].(string); ok {
				ws.completeSubscription(channel, resp.Arg, err)
				return
			}
		}
		if ws.hasPendingLogin() {
			ws.completeLogin(err)
		}
		return
	}

	if resp.Event == "login" {
		if resp.Code == "0" {
			ws.completeLogin(nil)
			ws.logger.Info("Login successful")
		} else {
			ws.completeLogin(ws.eventError(resp.Event, resp.Code, resp.Msg))
			ws.logger.Error("Login failed", "code", resp.Code, "msg", resp.Msg)
		}
		return
	}

	if resp.Event == "subscribe" {
		if resp.Arg != nil {
			if channel, ok := resp.Arg["channel"].(string); ok {
				var err error
				if resp.Code != "" && resp.Code != "0" {
					err = ws.eventError(resp.Event, resp.Code, resp.Msg)
				}
				ws.completeSubscription(channel, resp.Arg, err)
			}
		}
		ws.logger.Debug("Subscription event", "event", resp.Event, "arg", resp.Arg)
		return
	}

	if resp.Event == "unsubscribe" {
		ws.logger.Debug("Subscription event", "event", resp.Event, "arg", resp.Arg)
		return
	}

	if resp.Arg != nil {
		channel, ok := resp.Arg["channel"].(string)
		if !ok {
			ws.logger.Warn("No channel in message arg")
			return
		}

		subKey := ws.makeSubKeyFromArg(channel, resp.Arg)

		ws.mu.RLock()
		sub, exists := ws.subscriptions[subKey]
		ws.mu.RUnlock()

		if exists {
			select {
			case sub.ch <- message:
			default:
				ws.logger.Warn("Channel buffer full, dropping message", "channel", channel)
			}
		}
	}
}

func (ws *WSClient) handleReconnect() {
	backoff := time.Second
	maxBackoff := 60 * time.Second

	for {
		select {
		case <-ws.done:
			return
		default:
		}

		ws.logger.Info("Attempting to reconnect", "backoff", backoff)
		time.Sleep(backoff)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := ws.Connect(ctx)
		cancel()

		if err != nil {
			ws.logger.Error("Reconnect failed", "error", err)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		if ws.needsLogin() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := ws.Login(ctx); err != nil {
				ws.logger.Error("Re-authentication failed", "error", err)
				cancel()
				continue
			}
			cancel()
		}

		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		if err := ws.resubscribe(ctx); err != nil {
			ws.logger.Error("Resubscribe failed", "error", err)
			cancel()
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}
		cancel()

		ws.invokeReconnectCallbacks()
		ws.logger.Info("Reconnected successfully")
		return
	}
}

func (ws *WSClient) needsLogin() bool {
	ws.mu.RLock()
	defer ws.mu.RUnlock()
	return ws.loginRequested
}

func (ws *WSClient) resubscribe(ctx context.Context) error {
	ws.mu.RLock()
	subs := make([]wsSubscriptionSnapshot, 0, len(ws.subscriptions))
	for _, sub := range ws.subscriptions {
		subs = append(subs, wsSubscriptionSnapshot{
			key:     ws.makeSubKey(sub.channel, sub.args),
			channel: sub.channel,
			args:    cloneWSArgs(sub.args),
		})
	}
	ws.mu.RUnlock()

	for _, sub := range subs {
		ack := make(chan error, 1)
		ws.mu.Lock()
		current, exists := ws.subscriptions[sub.key]
		if !exists {
			ws.mu.Unlock()
			return fmt.Errorf("subscription disappeared before resubscribe: %s", sub.key)
		}
		current.ack = ack
		ws.mu.Unlock()

		req := models.WSSubscribeRequest{
			Op:   "subscribe",
			Args: []map[string]interface{}{ws.subscribeArgs(sub.channel, sub.args)},
		}
		if err := ws.send(req); err != nil {
			ws.clearSubscriptionAck(sub.key, ack)
			return fmt.Errorf("failed to resubscribe %s: %w", sub.channel, err)
		}
		if err := ws.waitSubscription(ctx, sub.key, ack); err != nil {
			return fmt.Errorf("failed to confirm resubscribe %s: %w", sub.channel, err)
		}
		ws.logger.Info("Resubscribed to channel", "channel", sub.channel, "args", sub.args)
	}
	return nil
}

func (ws *WSClient) invokeReconnectCallbacks() {
	ws.mu.RLock()
	callbacks := append([]func(){}, ws.reconnectCbs...)
	ws.mu.RUnlock()
	for _, cb := range callbacks {
		go cb()
	}
}

func (ws *WSClient) makeSubKey(channel string, args map[string]interface{}) string {
	key := channel
	if instID, ok := args["instId"].(string); ok {
		key += ":" + instID
	}
	if instType, ok := args["instType"].(string); ok {
		key += ":" + instType
	}
	if ccy, ok := args["ccy"].(string); ok {
		key += ":" + ccy
	}
	return key
}

func (ws *WSClient) makeSubKeyFromArg(channel string, arg map[string]interface{}) string {
	args := make(map[string]interface{})
	for k, v := range arg {
		if k != "channel" {
			args[k] = v
		}
	}
	return ws.makeSubKey(channel, args)
}
