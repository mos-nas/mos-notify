package main

import (
  "bytes"
  "crypto/tls"
  "encoding/json"
  "flag"
  "fmt"
  "io"
  "log"
  "net"
  "net/http"
  "net/smtp"
  "net/url"
  "os"
  "path/filepath"
  "strconv"
  "strings"
  "sync"
  "text/template"
  "time"

  "github.com/gorilla/websocket"
)

// Structures
type Message struct {
  ID        int64  `json:"id"`
  Title     string `json:"title"`
  Message   string `json:"message"`
  Priority  string `json:"priority"`
  Timestamp string `json:"timestamp"`
  Read      bool   `json:"read"`
}

type ProviderConfig struct {
  Enabled      bool                   `json:"enabled"`
  User         interface{}            `json:"user"`
  Token        interface{}            `json:"token"`
  Url          string                 `json:"url"`
  Method       string                 `json:"method"`
  Headers      map[string]string      `json:"headers"`
  Body         map[string]interface{} `json:"body"`
  AlertMapping map[string]interface{} `json:"alert_mapping"`
  ColorPrio    interface{}            `json:"color_prio"`
}

type EmailConfig struct {
  Enabled      bool                   `json:"enabled"`
  SmtpHost     string                 `json:"smtp_host"`
  SmtpPort     int                    `json:"smtp_port"`
  SmtpTLS      bool                   `json:"smtp_tls"`
  SmtpUser     string                 `json:"smtp_user"`
  SmtpPassword string                 `json:"smtp_password"`
  From         string                 `json:"from"`
  To           []string               `json:"to"`
  Subject      string                 `json:"subject"`
  Body         string                 `json:"body"`
  AlertMapping map[string]interface{} `json:"alert_mapping"`
}

type AppConfig struct {
  HttpPort int `json:"http_port"`
  WsPort   int `json:"ws_port"`
}

// Global vars
var (
	upgrader      = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	clients       = make(map[*websocket.Conn]bool)
	clientsMutex  sync.RWMutex
	broadcast     = make(chan Message, 100)
	providers     = make(map[string]ProviderConfig)
	emailCfg      *EmailConfig
	appcfg        = AppConfig{HttpPort: 999, WsPort: 999}
	fileMutex     = sync.Mutex{}
	logFilePath   = "/var/mos/notify/notifications.json"
	verbose       bool
)

// Main
func main() {
  flag.BoolVar(&verbose, "verbose", false, "Enable verbose logging for provider requests")
  flag.Parse()

  // Load ports.json
  if err := loadAppConfig("/boot/config/notify/ports.json"); err != nil {
    log.Printf("Couldn't load ports.json, using default ports: 999")
  }

  // Load providers
  cfgs, err := loadProviderConfigs("/boot/config/notify/providers")
  if err != nil {
    log.Printf("Warning: Could not load provider config: %v", err)
  }

  // Load email config separately
  if ec, err := loadEmailConfig("/boot/config/notify/providers/email.json"); err == nil {
    emailCfg = ec
  } else if !os.IsNotExist(err) {
    log.Printf("Warning: Could not load email config: %v", err)
  }

  if len(cfgs) == 0 && emailCfg == nil {
    log.Println("No providers configured - running in WebSocket-only mode")
  } else {
    providers = cfgs
    fmt.Println("Providers loaded:")
    for name, cfg := range providers {
      state := "enabled"
      if !cfg.Enabled {
        state = "disabled"
      }
      fmt.Printf("  - %s (%s)\n", name, state)
    }
    if emailCfg != nil {
      state := "enabled"
      if !emailCfg.Enabled {
        state = "disabled"
      }
      fmt.Printf("  - email (%s)\n", state)
    }
  }

  // Routes for receiving messages
  http.HandleFunc("/ws", handleConnections)
  http.HandleFunc("/send", handleSend)

  // Run dispatcher
  go handleMessages()

  // Run Unix Socket Server
  go startUnixSocketServer()

  addr := fmt.Sprintf(":%d", appcfg.HttpPort)
  fmt.Printf("Server start on port: %d\n", appcfg.HttpPort)
  fmt.Printf(" - WebSocket: ws://localhost:%d/ws\n", appcfg.WsPort)
  fmt.Printf(" - Send:    POST http://localhost:%d/send\n", appcfg.HttpPort)
  fmt.Printf(" - Socket:  echo 'Message' | /var/run/sock/mos-notify.sock\n")

  log.Fatal(http.ListenAndServe(addr, nil))
}

// Socket Server
func startUnixSocketServer() {
  socketPath := "/run/mos-notify.sock"

  // Create Socket path, shouldn't be necessary but just in case
  if err := os.MkdirAll("/run", 0755); err != nil {
    log.Printf("Konnte Socket-Verzeichnis nicht erstellen: %v", err)
    return
  }

  // Remove old Socket if exists
  os.Remove(socketPath)

  // Create Socket
  listener, err := net.Listen("unix", socketPath)
  if err != nil {
    log.Printf("Error creating Socket: %v", err)
    return
  }
  defer listener.Close()

  // chmod Socket for everyone
  os.Chmod(socketPath, 0666)

  log.Printf("Socket started: %s", socketPath)

  for {
    conn, err := listener.Accept()
    if err != nil {
      log.Printf("Socket Accept Error: %v", err)
      continue
    }

    go handleUnixSocketConnection(conn)
  }
}

// Socket handler
func handleUnixSocketConnection(conn net.Conn) {
  defer conn.Close()

  // Read message
  data := make([]byte, 1024)
  n, err := conn.Read(data)
  if err != nil {
    log.Printf("Socket read error: %v", err)
    return
  }

  messageText := strings.TrimSpace(string(data[:n]))
  if messageText == "" {
    return
  }

  if verbose {
    log.Printf("Socket message: %s", messageText)
  }

  // Try to parse JSON, if fail then handle as text
  var msg Message
  if err := json.Unmarshal([]byte(messageText), &msg); err != nil {
    msg = Message{
      Title:     "",
      Message:   messageText,
      Priority:  "normal",
      Timestamp: time.Now().Format(time.RFC3339Nano),
    }
    log.Printf("Socket text message: %s", messageText)
  } else {
    log.Printf("Socket json message: title='%s', message='%s', priority='%s'", msg.Title, msg.Message, msg.Priority)
  }

  // Validate and send on broadcast or fail silently
  if validateAndFixMessage(&msg) {
    broadcast <- msg
    // Just for troubleshooting
    //conn.Write([]byte("ok\n"))
  }
}

// WebSocket
func handleConnections(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Upgrade error: %v", err)
		return
	}
	defer ws.Close()

	clientsMutex.Lock()
	clients[ws] = true
	clientsMutex.Unlock()
	log.Println("Client connected")

	for {
		if _, _, err := ws.ReadMessage(); err != nil {
			clientsMutex.Lock()
			delete(clients, ws)
			clientsMutex.Unlock()
			log.Println("Client disconnected")
			break
		}
	}
}

// Message handler
func handleMessages() {
	for {
		msg := <-broadcast

		// Send to WebSocket - collect failed clients first
		var failedClients []*websocket.Conn
		clientsMutex.RLock()
		for client := range clients {
			if err := client.WriteJSON(msg); err != nil {
				client.Close()
				failedClients = append(failedClients, client)
			}
		}
		clientsMutex.RUnlock()

		// Remove failed clients
		if len(failedClients) > 0 {
			clientsMutex.Lock()
			for _, client := range failedClients {
				delete(clients, client)
			}
			clientsMutex.Unlock()
		}

		// Send to provider(s) if enabled
		for name, cfg := range providers {
			if !cfg.Enabled {
				continue
			}
			go sendToProvider(name, cfg, msg)
		}
		// Send email if configured and enabled
		if emailCfg != nil && emailCfg.Enabled {
			go sendEmail(*emailCfg, msg)
		}
		// Write to file
		go writeMessageToFile(msg)
	}
}

// HTTP /send
func handleSend(w http.ResponseWriter, r *http.Request) {
  body, _ := io.ReadAll(r.Body)
  defer r.Body.Close()
  var msg Message
  // Try to parse JSON, if fail then handle as text
  if strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
    if err := json.Unmarshal(body, &msg); err != nil {
      http.Error(w, "Invalid JSON", 400)
      return
    }
  } else {
    msg = Message{
      Title:    "",
      Message:  string(body),
      Priority: "normal",
    }
  }

  // Validate and send on broadcast or fail silently
  if validateAndFixMessage(&msg) {
    broadcast <- msg
    // Just for troubleshooting
    //conn.Write([]byte("ok\n"))
  }
}

// Message validation
func validateAndFixMessage(msg *Message) bool {
  if strings.TrimSpace(msg.Message) == "" {
    return false
  }
  if msg.Priority == "" {
    msg.Priority = "normal"
  }
  if msg.ID == 0 {
    msg.ID = time.Now().UnixMilli()
  }
  if msg.Timestamp == "" {
    msg.Timestamp = time.Now().Format(time.RFC3339Nano)
  }
  // Always include hostname in title
  if hn, err := os.Hostname(); err == nil {
    if strings.TrimSpace(msg.Title) == "" {
      msg.Title = hn
    } else {
      msg.Title = hn + " - " + msg.Title
    }
  } else {
    if strings.TrimSpace(msg.Title) == "" {
      msg.Title = "HostnameError"
    } else {
      msg.Title = "HostnameError - " + msg.Title
    }
  }
  return true
}

// Config loader
func loadAppConfig(path string) error {
  data, err := os.ReadFile(path)
  if err != nil {
    return err
  }
  return json.Unmarshal(data, &appcfg)
}

// Config loader for providers (skips email.json which is handled separately)
func loadProviderConfigs(dir string) (map[string]ProviderConfig, error) {
  configs := make(map[string]ProviderConfig)
  files, err := os.ReadDir(dir)
  if err != nil {
    return nil, err
  }
  for _, f := range files {
    if f.IsDir() || !strings.HasSuffix(f.Name(), ".json") {
      continue
    }
    if f.Name() == "email.json" {
      continue
    }
    data, err := os.ReadFile(filepath.Join(dir, f.Name()))
    if err != nil {
      return nil, fmt.Errorf("Config-Fehler %s: %v", f.Name(), err)
    }
    var cfg ProviderConfig
    if err := json.Unmarshal(data, &cfg); err != nil {
      return nil, fmt.Errorf("Config-Fehler %s: %v", f.Name(), err)
    }
    configs[strings.TrimSuffix(f.Name(), ".json")] = cfg
  }
  return configs, nil
}

// Config loader for email provider
func loadEmailConfig(filePath string) (*EmailConfig, error) {
  data, err := os.ReadFile(filePath)
  if err != nil {
    return nil, err
  }
  var cfg EmailConfig
  if err := json.Unmarshal(data, &cfg); err != nil {
    return nil, fmt.Errorf("email.json parse error: %v", err)
  }
  return &cfg, nil
}

// Email send function
func sendEmail(cfg EmailConfig, msg Message) {
  if cfg.SmtpHost == "" || cfg.From == "" || len(cfg.To) == 0 {
    if verbose {
      log.Printf("[email] Skipped: missing smtp_host, from, or to")
    }
    return
  }
  if cfg.SmtpPort == 0 {
    cfg.SmtpPort = 587
  }

  mapped := msg.Priority
  if cfg.AlertMapping != nil {
    if val, ok := cfg.AlertMapping[msg.Priority]; ok {
      mapped = fmt.Sprintf("%v", val)
    }
  }

  data := map[string]string{
    "Title":    msg.Title,
    "Message":  msg.Message,
    "Priority": mapped,
    "Time":     msg.Timestamp,
  }

  subject := renderTemplate(cfg.Subject, data)
  body := renderTemplate(cfg.Body, data)

  toHeader := strings.Join(cfg.To, ", ")
  mime := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=\"UTF-8\"\r\n\r\n%s",
    cfg.From, toHeader, subject, body)

  addr := fmt.Sprintf("%s:%d", cfg.SmtpHost, cfg.SmtpPort)

  var auth smtp.Auth
  if cfg.SmtpUser != "" {
    auth = smtp.PlainAuth("", cfg.SmtpUser, cfg.SmtpPassword, cfg.SmtpHost)
  }

  if cfg.SmtpPort == 465 {
    // Implicit TLS (SMTPS)
    tlsConfig := &tls.Config{ServerName: cfg.SmtpHost}
    conn, err := tls.Dial("tcp", addr, tlsConfig)
    if err != nil {
      log.Printf("[email] TLS connection failed: %v", err)
      return
    }
    defer conn.Close()

    client, err := smtp.NewClient(conn, cfg.SmtpHost)
    if err != nil {
      log.Printf("[email] SMTP client error: %v", err)
      return
    }
    defer client.Quit()

    if auth != nil {
      if err := client.Auth(auth); err != nil {
        log.Printf("[email] Auth failed: %v", err)
        return
      }
    }
    if err := client.Mail(cfg.From); err != nil {
      log.Printf("[email] MAIL FROM failed: %v", err)
      return
    }
    for _, rcpt := range cfg.To {
      if err := client.Rcpt(rcpt); err != nil {
        log.Printf("[email] RCPT TO <%s> failed: %v", rcpt, err)
        return
      }
    }
    w, err := client.Data()
    if err != nil {
      log.Printf("[email] DATA failed: %v", err)
      return
    }
    _, err = w.Write([]byte(mime))
    if err != nil {
      log.Printf("[email] Write failed: %v", err)
      return
    }
    w.Close()
  } else if cfg.SmtpTLS {
    // STARTTLS (typically port 587)
    client, err := smtp.Dial(addr)
    if err != nil {
      log.Printf("[email] Dial failed: %v", err)
      return
    }
    defer client.Quit()

    tlsConfig := &tls.Config{ServerName: cfg.SmtpHost}
    if err := client.StartTLS(tlsConfig); err != nil {
      log.Printf("[email] STARTTLS failed: %v", err)
      return
    }
    if auth != nil {
      if err := client.Auth(auth); err != nil {
        log.Printf("[email] Auth failed: %v", err)
        return
      }
    }
    if err := client.Mail(cfg.From); err != nil {
      log.Printf("[email] MAIL FROM failed: %v", err)
      return
    }
    for _, rcpt := range cfg.To {
      if err := client.Rcpt(rcpt); err != nil {
        log.Printf("[email] RCPT TO <%s> failed: %v", rcpt, err)
        return
      }
    }
    w, err := client.Data()
    if err != nil {
      log.Printf("[email] DATA failed: %v", err)
      return
    }
    _, err = w.Write([]byte(mime))
    if err != nil {
      log.Printf("[email] Write failed: %v", err)
      return
    }
    w.Close()
  } else {
    // Plain SMTP (no TLS)
    err := smtp.SendMail(addr, auth, cfg.From, cfg.To, []byte(mime))
    if err != nil {
      log.Printf("[email] SendMail failed: %v", err)
      return
    }
  }

  if verbose {
    log.Printf("[email] Sent to %s (subject: %s)", toHeader, subject)
  }
}

// Provider send function
func sendToProvider(name string, cfg ProviderConfig, msg Message) {
  if cfg.Url == "" {
    if verbose {
      log.Printf("[%s] Skipped: empty URL", name)
    }
    return
  }
  if cfg.Method == "" {
    cfg.Method = "POST"
  }

  mapped := msg.Priority
  if cfg.AlertMapping != nil {
    if val, ok := cfg.AlertMapping[msg.Priority]; ok {
      mapped = fmt.Sprintf("%v", val)
    }
  }
  data := buildTemplateData(msg, cfg, mapped)
  // Render body
  result := make(map[string]interface{})
  for k, v := range cfg.Body {
    result[k] = renderTemplateRecursive(v, data)
  }
  var reqBody io.Reader
  if ct, ok := cfg.Headers["Content-Type"]; ok && strings.Contains(ct, "json") {
    b, _ := json.Marshal(result)
    reqBody = bytes.NewBuffer(b)
    if verbose {
      log.Printf("[%s] Request body: %s", name, string(b))
    }
  } else {
    form := url.Values{}
    for k, v := range result {
      form.Set(k, fmt.Sprintf("%v", v))
    }
    reqBody = strings.NewReader(form.Encode())
    if verbose {
      log.Printf("[%s] Request body (form): %s", name, form.Encode())
    }
  }
  finalURL := renderTemplate(cfg.Url, data)
  req, err := http.NewRequest(cfg.Method, finalURL, reqBody)
  if err != nil {
    log.Printf("[%s] Error creating request: %v", name, err)
    return
  }
  for k, v := range cfg.Headers {
    req.Header.Set(k, renderTemplate(v, data))
  }
  if verbose {
    log.Printf("[%s] %s %s", name, cfg.Method, finalURL)
  }
  resp, err := http.DefaultClient.Do(req)
  if err != nil {
    log.Printf("[%s] Request failed: %v", name, err)
    return
  }
  defer resp.Body.Close()
  if resp.StatusCode >= 400 {
    respBody, _ := io.ReadAll(resp.Body)
    log.Printf("[%s] Provider returned %s: %s", name, resp.Status, string(respBody))
  } else if verbose {
    log.Printf("[%s] Success: %s", name, resp.Status)
  }
}

// Helper
func buildTemplateData(msg Message, cfg ProviderConfig, mappedPriority string) map[string]string {
  data := map[string]string{
    "Title":    msg.Title,
    "Message":  msg.Message,
    "Priority": mappedPriority,
    "Time":     msg.Timestamp,
  }

  // Add color information if color_prio contains mappings
  if colorMap, ok := cfg.ColorPrio.(map[string]interface{}); ok && colorMap != nil {
    if colorVal, exists := colorMap[msg.Priority]; exists {
      if color, isString := colorVal.(string); isString {
        data["Color"] = color
        data["ColorHex"] = color // Alias for hex colors
      }
    } else {
      // Default color if priority not found in mapping
      data["Color"] = "8421504"
      data["ColorHex"] = "8421504"
    }
  } else {
    // Provide default color even when color_prio is not set
    data["Color"] = "8421504"
    data["ColorHex"] = "8421504"
  }

  if u, ok := cfg.User.(string); ok && u != "" {
    data["User"] = u
  }
  if t, ok := cfg.Token.(string); ok && t != "" {
    data["Token"] = t
  }
  return data
}

func renderTemplate(tmpl string, data map[string]string) string {
  t, err := template.New("tmpl").Parse(tmpl)
  if err != nil {
    return tmpl
  }
  var buf bytes.Buffer
  if err := t.Execute(&buf, data); err != nil {
    return tmpl
  }
  return buf.String()
}

func renderTemplateRecursive(v interface{}, data map[string]string) interface{} {
  switch val := v.(type) {
  case string:
    return renderTemplate(val, data)
  case map[string]interface{}:
    if numTempl, ok := val["$number"].(string); ok {
      rendered := renderTemplate(numTempl, data)
      if num, err := strconv.Atoi(rendered); err == nil {
        return num
      } else {
        return rendered
      }
    } else {
      result := make(map[string]interface{})
      for k, v := range val {
        result[k] = renderTemplateRecursive(v, data)
      }
      return result
    }
  case []interface{}:
    result := make([]interface{}, len(val))
    for i, item := range val {
      result[i] = renderTemplateRecursive(item, data)
    }
    return result
  default:
    return val
  }
}

// File logging function
func writeMessageToFile(msg Message) {
  fileMutex.Lock()
  defer fileMutex.Unlock()

  // Ensure directory exists
  dir := filepath.Dir(logFilePath)
  if err := os.MkdirAll(dir, 0755); err != nil {
    log.Printf("Error creating directory %s: %v", dir, err)
    return
  }

  var messages []Message

  // Read existing messages if file exists
  if data, err := os.ReadFile(logFilePath); err == nil {
    if err := json.Unmarshal(data, &messages); err != nil {
      log.Printf("Error parsing existing notifications file: %v", err)
      messages = []Message{} // Start fresh if file is corrupted
    }
  }

  // Append new message
  messages = append(messages, msg)

  // Keep only last 1000 messages
  if len(messages) > 1000 {
    messages = messages[len(messages)-1000:]
  }

  // Write back to file
  data, err := json.MarshalIndent(messages, "", "  ")
  if err != nil {
    log.Printf("Error marshaling messages: %v", err)
    return
  }

  if err := os.WriteFile(logFilePath, data, 0644); err != nil {
    log.Printf("Error writing to notifications file: %v", err)
    return
  }
}

// go mod tidy && go build -ldflags="-s -w" -o mos-notify
