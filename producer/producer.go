package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"time"
)

type EnqueueRequest struct {
	UserID   string          `json:"user_id"`
	Priority int             `json:"priority,omitempty"`
	Payload  json.RawMessage `json:"payload"`
}

type EnqueueResponse struct {
	JobID string `json:"job_id"`
	OK    bool   `json:"ok"`
}

func main() {
	serverURL := getenv("SERVER_URL", "http://127.0.0.1:8080")
	userID := getenv("USER_ID", fmt.Sprintf("user-%d", time.Now().UnixNano()%1000))
	total := getenvInt("TOTAL", 50) // nombre total de jobs à envoyer
	block := getenvInt("BLOCK", 10) // taille d’un bloc (pour simuler “50 puis 50”, etc.)
	minPause := getenvDuration("MIN_PAUSE", 100*time.Millisecond)
	maxPause := getenvDuration("MAX_PAUSE", 1500*time.Millisecond)
	priority := getenvInt("PRIORITY", 0)

	log.Printf("provider -> %s | user=%s | total=%d block=%d", serverURL, userID, total, block)

	client := &http.Client{Timeout: 5 * time.Second}

	sent := 0
	for sent < total {
		batch := min(block, total-sent)
		for i := 0; i < batch; i++ {
			payload := map[string]any{
				"page":  sent + i,
				"pdf":   "file.pdf",
				"note":  "demo",
				"sleep": rand.IntN(3000), // info arbitraire
			}
			body, _ := json.Marshal(payload)

			reqBody := EnqueueRequest{
				UserID:   userID,
				Priority: priority,
				Payload:  body,
			}
			buf, _ := json.Marshal(reqBody)

			resp, err := client.Post(serverURL+"/enqueue", "application/json", bytes.NewReader(buf))
			if err != nil {
				log.Printf("POST error: %v", err)
				continue
			}
			var enq EnqueueResponse
			_ = json.NewDecoder(resp.Body).Decode(&enq)
			resp.Body.Close()
			log.Printf("enqueued job_id=%s ok=%v", enq.JobID, enq.OK)

			// Pause aléatoire entre jobs
			time.Sleep(randBetween(minPause, maxPause))
		}

		sent += batch
		// Pause entre “blocs”
		time.Sleep(randBetween(500*time.Millisecond, 2*time.Second))
	}
	log.Printf("provider done (sent=%d)", sent)
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func getenvInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		var x int
		fmt.Sscanf(v, "%d", &x)
		return x
	}
	return def
}

func getenvDuration(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	return def
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func randBetween(a, b time.Duration) time.Duration {
	if b <= a {
		return a
	}
	delta := b - a
	return a + time.Duration(rand.Int64N(int64(delta)))
}
