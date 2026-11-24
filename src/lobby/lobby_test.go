package lobby

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestLobbyStartPageAndPost(t *testing.T) {
	server, url, err := Start("127.0.0.1:0", "test-secret-code", "croc --fanout test-secret-code")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })

	if !strings.HasPrefix(url, "http://127.0.0.1:") {
		t.Fatalf("unexpected lobby URL: %s", url)
	}

	page, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer page.Body.Close()
	body, err := io.ReadAll(page.Body)
	if err != nil {
		t.Fatal(err)
	}
	if page.StatusCode != http.StatusOK {
		t.Fatalf("page status = %d", page.StatusCode)
	}
	text := string(body)
	if !strings.Contains(text, "test-secret-code") || !strings.Contains(text, "croc --fanout test-secret-code") {
		t.Fatalf("lobby page missing receive details")
	}

	eventURL := strings.TrimSuffix(url, "/") + "/events"
	eventClient := &http.Client{Timeout: 2 * time.Second}
	eventResponse, err := eventClient.Get(eventURL)
	if err != nil {
		t.Fatal(err)
	}
	defer eventResponse.Body.Close()

	response, err := http.Post(
		strings.TrimSuffix(url, "/")+"/message",
		"application/json",
		strings.NewReader(`{"name":"Alice","text":"hello"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("message status = %d", response.StatusCode)
	}
}
