package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

type EnqueueRequest struct {
	UserID   string          `json:"user_id"`            // ex: "userA"
	Priority int             `json:"priority,omitempty"` // 0 = normal (tu peux t’en servir plus tard)
	Payload  json.RawMessage `json:"payload"`            // arbitraire (ex: { "page": 42, "pdf": "file.pdf" })
}

type EnqueueResponse struct {
	JobID string `json:"job_id"`
	OK    bool   `json:"ok"`
}

func maybeStartEmbeddedNATS() *natsserver.Server {
	if os.Getenv("START_NATS") == "" {
		return nil // ne lance pas si non demandé
	}

	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      4222,          // NATS
		HTTPPort:  8222,          // monitoring (facultatif)
		JetStream: true,          // nécessaire pour JS
		StoreDir:  "./nats-data", // persistance JetStream
		NoLog:     false,
		NoSigs:    true, // évite d’attraper les signaux dans un binaire embarqué
	}

	s, err := natsserver.NewServer(opts)
	if err != nil {
		log.Fatalf("nats embed: %v", err)
	}
	go s.Start()

	// attend que ça écoute
	if !s.ReadyForConnections(10 * time.Second) {
		log.Fatalf("nats embed: not ready in time")
	}
	log.Printf("NATS embedded started on %s:%d (JS=%v)", opts.Host, opts.Port, opts.JetStream)
	return s
}

func main() {
	// 1) Démarre NATS embarqué si START_NATS=1
	s := maybeStartEmbeddedNATS()
	defer func() {
		if s != nil {
			s.Shutdown()
		}
	}()

	// 2) Ensuite, connecte tes clients comme avant
	natsURL := getenv("NATS_URL", nats.DefaultURL)
	addr := getenv("SERVER_ADDR", ":8080")

	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("connect NATS: %v", err)
	}
	defer nc.Drain()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("jetstream: %v", err)
	}

	// Crée/assure le stream "JOBS" (WorkQueue = messages retirés après ACK explicite)
	_, err = js.AddStream(&nats.StreamConfig{
		Name:      "JOBS",
		Subjects:  []string{"jobs.>"},
		Retention: nats.WorkQueuePolicy,
		Storage:   nats.FileStorage,
		MaxAge:    0, // conserve jusqu’à ack (pas de TTL)
	})
	if err != nil && err != nats.ErrStreamNameAlreadyInUse {
		log.Fatalf("add stream: %v", err)
	}

	// Handler HTTP /enqueue
	http.HandleFunc("/enqueue", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var req EnqueueRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}
		if req.UserID == "" {
			http.Error(w, "missing user_id", http.StatusBadRequest)
			return
		}
		jobID := uuid.NewString()

		// Enveloppe du message
		msg := map[string]any{
			"job_id":   jobID,
			"user_id":  req.UserID,
			"priority": req.Priority,
			"payload":  json.RawMessage(req.Payload),
			"ts":       time.Now().UTC().Format(time.RFC3339Nano),
		}
		b, _ := json.Marshal(msg)

		// Subject simple (tu pourras affiner par user: jobs.user.<uid>)
		subject := "jobs.pages"

		// Ajoute quelques headers utiles
		hdr := nats.Header{}
		hdr.Set("job-id", jobID)
		hdr.Set("user-id", req.UserID)
		hdr.Set("priority", fmt.Sprintf("%d", req.Priority))

		pa := &nats.Msg{
			Subject: subject,
			Header:  hdr,
			Data:    b,
		}

		if _, err := js.PublishMsg(pa); err != nil {
			http.Error(w, "publish: "+err.Error(), http.StatusInternalServerError)
			return
		}

		resp := EnqueueResponse{JobID: jobID, OK: true}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	log.Printf("server listening on %s (NATS=%s)", addr, natsURL)
	log.Fatal(http.ListenAndServe(addr, nil))
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
