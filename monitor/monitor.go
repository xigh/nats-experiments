package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	natsURL := getenv("NATS_URL", nats.DefaultURL)
	stream := getenv("STREAM", "JOBS")
	refresh := getenvDuration("REFRESH", 500*time.Millisecond)
	showAdvisories := getenvBool("ADVISORIES", false)

	log.Printf("monitor connecting NATS=%s stream=%s refresh=%s", natsURL, stream, refresh)

	nc, err := nats.Connect(natsURL, nats.Name("monitor"))
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer nc.Drain()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("jetstream: %v", err)
	}

	// Optionnel : écoute des advisories JetStream
	var sub *nats.Subscription
	if showAdvisories {
		sub, err = nc.Subscribe("$JS.EVENT.>", func(m *nats.Msg) {
			fmt.Printf("[EVENT] %s %s\n", m.Subject, truncate(string(m.Data), 200))
		})
		if err != nil {
			log.Printf("advisories sub error: %v", err)
		}
		defer func() {
			if sub != nil {
				_ = sub.Unsubscribe()
			}
		}()
	}

	// Gestion des signaux pour quitter proprement
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	t := time.NewTicker(refresh)
	defer t.Stop()

	clear := func() {
		if isTTY() {
			fmt.Print("\033[H\033[2J")
		}
	}

	for {
		select {
		case <-t.C:
			clear()
			printSnapshot(js, stream)
		case <-sigCh:
			log.Println("monitor: received signal, exiting")
			return
		}
	}
}

func printSnapshot(js nats.JetStreamContext, stream string) {
	info, err := js.StreamInfo(stream)
	if err != nil {
		fmt.Printf("stream %q: %v\n", stream, err)
		return
	}

	// --- STREAM ---
	st := info.State
	fmt.Printf("=== STREAM %-16s ===\n", stream)
	fmt.Printf("Msgs: %-8d  Bytes: %-10d  Consumers: %-4d  FirstSeq: %-8d  LastSeq: %-8d  Deleted: %-6d  Subjects: %v\n",
		st.Msgs, st.Bytes, st.Consumers, st.FirstSeq, st.LastSeq, st.NumDeleted, info.Config.Subjects)

	fmt.Printf("QueueDepth: %d   (WorkQueuePolicy ⇒ messages disparaissent après ACK)\n", st.Msgs)
	fmt.Println()

	// --- CONSUMERS ---
	fmt.Println("=== CONSUMERS ===")
	names := listConsumers(js, stream)
	if len(names) == 0 {
		fmt.Println("(no consumers)")
		return
	}

	fmt.Printf("%-24s %-8s %-10s %-10s %-10s %-12s %-12s %-12s\n",
		"Name", "AckWait", "AckPend", "NumWait", "NumPend", "Delivered", "Redelivered", "Filter")

	for _, name := range names {
		ci, err := js.ConsumerInfo(stream, name)
		if err != nil {
			fmt.Printf("%-24s ERROR: %v\n", name, err)
			continue
		}
		cfg := ci.Config
		// Delivered.Stream est le dernier seq du stream délivré à ce consumer
		fmt.Printf("%-24s %-8s %-10d %-10d %-10d %-12d %-12d %-12s\n",
			name,
			cfg.AckWait,
			ci.NumAckPending,
			ci.NumWaiting,
			ci.NumPending,
			ci.Delivered.Stream,
			ci.NumRedelivered, // <- champ correct
			emptyIf(cfg.FilterSubject, "*"),
		)
	}
	fmt.Println()

	// Hints rapides
	printHints(info, names)
}

func listConsumers(js nats.JetStreamContext, stream string) []string {
	ch := js.Consumers(stream) // <- renvoie <-chan *nats.ConsumerInfo dans ta version
	var names []string
	for ci := range ch {
		if ci == nil {
			break
		}
		names = append(names, ci.Name)
	}
	return names
}

func printHints(info *nats.StreamInfo, names []string) {
	if info.State.Msgs > 0 && info.State.Consumers == 0 {
		fmt.Println("⚠️  Des messages sont en attente mais aucun consumer n'est attaché.")
	}
	if info.State.Msgs > 1000 {
		fmt.Println("ℹ️  Beaucoup de messages en file ; augmente le nombre de consumers ou la taille de batch pull.")
	}
	if len(names) > 1 {
		fmt.Println("ℹ️  Plusieurs consumers : vérifie qu'ils partagent un durable/pull-sub pour ‘work sharing’.")
	}
	fmt.Println("Tips: ajuste AckWait vs durée réelle des jobs ; utilise msg.InProgress() pour étendre la fenêtre d’ACK ; surveille NumRedelivered/NumAckPending.")
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func getenvDuration(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func getenvBool(k string, def bool) bool {
	if v := os.Getenv(k); v != "" {
		switch strings.ToLower(v) {
		case "1", "true", "yes", "y", "on":
			return true
		case "0", "false", "no", "n", "off":
			return false
		}
	}
	return def
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func emptyIf(s, alt string) string {
	if s == "" {
		return alt
	}
	return s
}

func isTTY() bool {
	// petite heuristique ; on laisse à true pour forcer le clear écran dans un terminal
	return true
}
