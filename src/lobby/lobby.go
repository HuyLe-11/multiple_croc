package lobby

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Server struct {
	server   *http.Server
	listener net.Listener
	room     string
	code     string
	command  string

	mu      sync.Mutex
	clients map[chan []byte]struct{}
}

type message struct {
	Name string `json:"name"`
	Text string `json:"text"`
	Time string `json:"time"`
}

func Start(bindAddress, code, receiverCommand string) (*Server, string, error) {
	listener, err := net.Listen("tcp", bindAddress)
	if err != nil {
		return nil, "", fmt.Errorf("start lobby listener: %w", err)
	}

	roomBytes := make([]byte, 16)
	if _, err = rand.Read(roomBytes); err != nil {
		_ = listener.Close()
		return nil, "", fmt.Errorf("create lobby room: %w", err)
	}
	room := hex.EncodeToString(roomBytes)

	s := &Server{
		listener: listener,
		room:     room,
		code:     code,
		command:  receiverCommand,
		clients:  make(map[chan []byte]struct{}),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/lobby/", s.handle)
	s.server = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      0,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		_ = s.server.Serve(listener)
	}()

	urlHost := listener.Addr().(*net.TCPAddr).IP.String()
	if urlHost == "" || urlHost == "<nil>" || urlHost == "0.0.0.0" || urlHost == "::" {
		urlHost = firstLANIP()
	}
	port := listener.Addr().(*net.TCPAddr).Port
	return s, fmt.Sprintf("http://%s:%d/lobby/%s", urlHost, port, room), nil
}

func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return s.server.Shutdown(ctx)
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/lobby/")
	roomEnd := strings.IndexByte(path, '/')
	room := path
	subpath := ""
	if roomEnd >= 0 {
		room = path[:roomEnd]
		subpath = path[roomEnd:]
	}
	if room != s.room {
		http.NotFound(w, r)
		return
	}

	switch {
	case subpath == "" && r.Method == http.MethodGet:
		s.servePage(w)
	case subpath == "/events" && r.Method == http.MethodGet:
		s.serveEvents(w, r)
	case subpath == "/message" && r.Method == http.MethodPost:
		s.postMessage(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) servePage(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = fmt.Fprintf(w, `<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>croc fan-out lobby</title><style>
body{font-family:system-ui,sans-serif;background:#111827;color:#e5e7eb;margin:0}.wrap{max-width:760px;margin:auto;padding:24px}
.card{background:#1f2937;border:1px solid #374151;border-radius:12px;padding:16px;margin:12px 0}.code,.command{font-family:monospace;word-break:break-all}
#messages{height:360px;overflow:auto}.msg{border-bottom:1px solid #374151;padding:8px 0}.name{color:#60a5fa;font-weight:bold}
form{display:flex;gap:8px}input{box-sizing:border-box;border:1px solid #4b5563;border-radius:8px;background:#111827;color:white;padding:10px}
#name{width:150px}#text{flex:1}button{border:0;border-radius:8px;background:#2563eb;color:white;padding:10px 16px;cursor:pointer}
</style></head><body><div class="wrap"><h1>croc fan-out lobby</h1>
<div class="card"><b>Receive code</b><div class="code">%s</div><b>CLI command</b><div class="command" id="cli-command">%s</div><button type="button" id="copy-command">Copy command</button></div>
<div class="card"><div id="messages"></div><form id="chat"><input id="name" maxlength="40" placeholder="Name" required>
<input id="text" maxlength="2000" placeholder="Message" required><button>Send</button></form></div></div>
<script>
const messages=document.querySelector("#messages");
function appendMessage(m){
 const d=document.createElement("div");
 d.className="msg";
 const name=document.createElement("span");
 name.className="name";
 name.textContent=m.name+" ";
 const text=document.createElement("span");
 text.textContent=m.text;
 d.append(name);
 if(m.time){d.append(document.createTextNode(m.time+" "));}
 d.append(text);
 messages.append(d);
 messages.scrollTop=messages.scrollHeight;
}
const events=new EventSource(location.pathname+"/events");
events.onmessage=e=>appendMessage(JSON.parse(e.data));
document.querySelector("#copy-command").addEventListener("click",async()=>{
 const command=document.querySelector("#cli-command").textContent;
 await navigator.clipboard.writeText(command);
 const button=document.querySelector("#copy-command");
 button.textContent="Copied";
 setTimeout(()=>button.textContent="Copy command",1500);
});
document.querySelector("#chat").addEventListener("submit",async e=>{
 e.preventDefault();
 const name=document.querySelector("#name").value.trim();
 const text=document.querySelector("#text").value.trim();
 if(!name||!text)return;
 const button=e.submitter;
 if(button)button.disabled=true;
 try{
  const response=await fetch(location.pathname+"/message",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({name,text})});
  if(!response.ok)throw new Error("HTTP "+response.status);
  appendMessage({name,text,time:new Date().toLocaleTimeString([],{hour:"2-digit",minute:"2-digit"})});
  document.querySelector("#text").value="";
 }catch(error){
  alert("Could not send message: "+error.message);
 }finally{
  if(button)button.disabled=false;
 }
});
</script></body></html>`,
		html.EscapeString(s.code), html.EscapeString(s.command))
}

func (s *Server) serveEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := make(chan []byte, 16)
	s.mu.Lock()
	s.clients[ch] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.clients, ch)
		s.mu.Unlock()
	}()

	_, _ = w.Write([]byte(": connected\n\n"))
	flusher.Flush()
	for {
		select {
		case payload := <-ch:
			_, _ = fmt.Fprintf(w, "data: %s\n\n", payload)
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

func (s *Server) postMessage(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	var incoming message
	if err := json.NewDecoder(r.Body).Decode(&incoming); err != nil {
		http.Error(w, "invalid message", http.StatusBadRequest)
		return
	}
	incoming.Name = strings.TrimSpace(incoming.Name)
	incoming.Text = strings.TrimSpace(incoming.Text)
	if len(incoming.Name) == 0 || len(incoming.Name) > 40 || len(incoming.Text) == 0 || len(incoming.Text) > 2000 {
		http.Error(w, "invalid name or message", http.StatusBadRequest)
		return
	}
	incoming.Time = time.Now().Format("15:04")
	payload, err := json.Marshal(incoming)
	if err != nil {
		http.Error(w, "encode message", http.StatusInternalServerError)
		return
	}

	s.mu.Lock()
	for ch := range s.clients {
		select {
		case ch <- payload:
		default:
		}
	}
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func firstLANIP() string {
	interfaces, err := net.Interfaces()
	if err != nil {
		return "127.0.0.1"
	}
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			ipNet, ok := addr.(*net.IPNet)
			if !ok {
				continue
			}
			ip := ipNet.IP
			if ip.To4() != nil && !ip.IsLoopback() {
				return ip.String()
			}
		}
	}
	return "127.0.0.1"
}
