package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand/v2"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

type Job struct {
	JobID    string          `json:"job_id"`
	UserID   string          `json:"user_id"`
	Payload  json.RawMessage `json:"payload"`
	Priority int             `json:"priority"`
	TS       string          `json:"ts"`
}

func main() {
	natsURL := getenv("NATS_URL", nats.DefaultURL)
	ackWait := getenvDuration("ACK_WAIT", 5*time.Second)   // fenêtre pour ack
	hbEvery := getenvDuration("HB_EVERY", 3*time.Second)   // fréquence des heartbeats InProgress
	maxWait := getenvDuration("FETCH_WAIT", 2*time.Second) // timeout de fetch
	durable := getenv("DURABLE", "workers")
	stream := getenv("STREAM", "JOBS")
	subject := getenv("SUBJECT", "jobs.pages")

	log.Printf("consumer starting (NATS=%s stream=%s subj=%s durable=%s)", natsURL, stream, subject, durable)

	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer nc.Drain()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("jetstream: %v", err)
	}

	// Assure le stream existe (au cas où server n’est pas lancé)
	_, _ = js.AddStream(&nats.StreamConfig{
		Name:      stream,
		Subjects:  []string{"jobs.>"},
		Retention: nats.WorkQueuePolicy,
		Storage:   nats.FileStorage,
	})

	// Crée/assure un consumer durable en Pull
	_, _ = js.AddConsumer(stream, &nats.ConsumerConfig{
		Durable:       durable,
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       ackWait, // délai de redelivery si pas d’ACK
		MaxAckPending: 1024,
		FilterSubject: subject,
		DeliverPolicy: nats.DeliverAllPolicy,
		ReplayPolicy:  nats.ReplayInstantPolicy,
	})

	sub, err := js.PullSubscribe(subject, durable, nats.BindStream(stream))
	if err != nil {
		log.Fatalf("pull subscribe: %v", err)
	}

	for {
		// Fetch 1 message à la fois (scalable avec plusieurs instances)
		msgs, err := sub.Fetch(1, nats.MaxWait(maxWait))
		if err != nil {
			// Timeout normal si rien à consommer
			if err == nats.ErrTimeout {
				continue
			}
			log.Printf("fetch error: %v", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		for _, m := range msgs {
			go handleOne(m, ackWait, hbEvery)
		}
	}
}

func handleOne(m *nats.Msg, ackWait, hbEvery time.Duration) {
	start := time.Now()
	var j Job
	if err := json.Unmarshal(m.Data, &j); err != nil {
		log.Printf("bad job json: %v", err)
		_ = m.Ack() // on ack pour éviter une redelivery infinie sur message illisible
		return
	}
	log.Printf("got job id=%s user=%s", j.JobID, j.UserID)

	// Durée de traitement aléatoire (1..12 s)
	work := time.Duration(1+rand.IntN(12)) * time.Second
	// Probabilité de demander explicitement plus de temps (heartbeat)
	needHB := work > ackWait || rand.IntN(100) < 25

	// Tâche de heartbeat pour étendre la fenêtre (InProgress reset le compteur d’AckWait)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if needHB {
		go func() {
			t := time.NewTicker(hbEvery)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					if err := m.InProgress(); err != nil {
						log.Printf("in-progress err job=%s: %v", j.JobID, err)
					} else {
						log.Printf("heartbeat (in-progress) job=%s", j.JobID)
					}
				}
			}
		}()
	}

	// Simule le travail
	time.Sleep(work)

	// Simulation de panne aléatoire (10%) : on NAK pour redelivery plus tard
	if rand.IntN(100) < 10 {
		delay := 2 * time.Second
		if err := m.NakWithDelay(delay); err != nil {
			log.Printf("nak err job=%s: %v", j.JobID, err)
		} else {
			log.Printf("job=%s NAK (retry in %s), worked=%s", j.JobID, delay, time.Since(start))
		}
		return
	}

	// Terminé : ACK
	if err := m.Ack(); err != nil {
		log.Printf("ack err job=%s: %v", j.JobID, err)
		return
	}
	cancel()
	log.Printf("job=%s DONE (worked=%s, needHB=%v)", j.JobID, time.Since(start), needHB)
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
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
