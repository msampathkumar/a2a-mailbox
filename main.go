package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/msampathkumar/a2a-mailbox/internal/server"
	"github.com/msampathkumar/a2a-mailbox/internal/store"
)

const version = "0.1.0"

type pluginInfo struct {
	Name        string `json:"name"`
	Version     string `json:"version"`
	Description string `json:"description"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, out io.Writer, errOut io.Writer) int {
	if len(args) > 0 {
		switch args[0] {
		case "info":
			data, _ := json.Marshal(pluginInfo{
				Name:        "mailbox",
				Version:     version,
				Description: "Executor-less A2A inbox server backed by local DB, tk tickets, or Pub/Sub",
			})
			fmt.Fprintln(out, string(data))
			return 0
		case "--version", "-v", "version":
			fmt.Fprintf(out, "a2a-mailbox %s\n", version)
			return 0
		case "serve":
			return runServe(ctx, args[1:], out, errOut)
		case "list":
			return runList(ctx, args[1:], out, errOut)
		case "show":
			return runShow(ctx, args[1:], out, errOut)
		case "reply":
			return runReply(ctx, args[1:], out, errOut)
		case "--help", "-h", "help":
			printUsage(out)
			return 0
		}
	}

	printUsage(out)
	return 0
}

func defaultDBPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "mailbox.json"
	}
	return filepath.Join(home, ".config", "a2a-mailbox", "mailbox.json")
}

func openBackend(backendType, dbPath, tkDir, tkBin, pubsubEndpoint, pubsubProject, pubsubTopic, pubsubToken, ackState string) (store.Backend, error) {
	if dbPath == "" {
		dbPath = defaultDBPath()
	}
	state := normalizeState(ackState)
	db, err := store.NewDBBackend(dbPath, state)
	if err != nil {
		return nil, err
	}
	switch strings.ToLower(strings.TrimSpace(backendType)) {
	case "", "db":
		return db, nil
	case "tk":
		if tkDir == "" {
			cwd, err := os.Getwd()
			if err != nil {
				return nil, err
			}
			tkDir = cwd
		}
		return store.NewTKBackend(db, tkBin, tkDir)
	case "pubsub":
		return store.NewPubSubBackend(db, pubsubEndpoint, pubsubProject, pubsubTopic, pubsubToken)
	default:
		return nil, fmt.Errorf("unknown backend %q (choose db, tk, or pubsub)", backendType)
	}
}

func normalizeState(s string) string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "input-required", "input_required", "task_state_input_required":
		return "TASK_STATE_INPUT_REQUIRED"
	case "submitted", "task_state_submitted":
		return "TASK_STATE_SUBMITTED"
	case "working", "in_progress", "in-progress", "task_state_working":
		return "TASK_STATE_WORKING"
	case "completed", "closed", "done", "task_state_completed":
		return "TASK_STATE_COMPLETED"
	case "canceled", "cancelled", "task_state_canceled":
		return "TASK_STATE_CANCELED"
	case "failed", "task_state_failed":
		return "TASK_STATE_FAILED"
	default:
		return strings.ToUpper(strings.TrimSpace(s))
	}
}

func runServe(ctx context.Context, args []string, out io.Writer, errOut io.Writer) int {
	fs := flag.NewFlagSet("a2a-mailbox serve", flag.ContinueOnError)
	fs.SetOutput(errOut)
	host := fs.String("host", "127.0.0.1", "Bind host address (default 127.0.0.1)")
	port := fs.Int("port", 8085, "Listen port")
	backendType := fs.String("backend", "db", "Storage backend: db, tk, or pubsub")
	dbPath := fs.String("db", "", "Path to local mailbox JSON database")
	tkDir := fs.String("tk-dir", "", "Working directory for tk tickets (default: current directory)")
	tkBin := fs.String("tk-bin", "tk", "Path to tk binary")
	pubsubEndpoint := fs.String("pubsub-endpoint", "", "Google Cloud Pub/Sub REST endpoint override")
	pubsubProject := fs.String("pubsub-project", os.Getenv("GOOGLE_CLOUD_PROJECT"), "Google Cloud project ID for Pub/Sub")
	pubsubTopic := fs.String("pubsub-topic", "a2a-mailbox", "Google Cloud Pub/Sub topic ID")
	ackState := fs.String("ack-state", "input-required", "Initial A2A task state on message registration (input-required, working, submitted)")
	name := fs.String("name", "A2A Mailbox", "Agent Card name")
	desc := fs.String("description", "Executor-less A2A inbox server that registers and stores incoming tasks", "Agent Card description")

	if err := fs.Parse(args); err != nil {
		return 2
	}

	b, err := openBackend(
		*backendType,
		*dbPath,
		*tkDir,
		*tkBin,
		*pubsubEndpoint,
		*pubsubProject,
		*pubsubTopic,
		os.Getenv("PUBSUB_BEARER_TOKEN"),
		*ackState,
	)
	if err != nil {
		fmt.Fprintf(errOut, "Error initializing backend: %v\n", err)
		return 1
	}

	addr := fmt.Sprintf("%s:%d", *host, *port)
	baseURL := fmt.Sprintf("http://%s", addr)
	handler := server.NewHandler(server.Config{
		Name:        *name,
		Description: *desc,
		Version:     version,
		BaseURL:     baseURL,
		Backend:     b,
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		fmt.Fprintf(out, "A2A Mailbox listening on %s (backend=%s, card=%s/.well-known/agent-card.json)\n", baseURL, *backendType, baseURL)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
		return 0
	case err := <-errCh:
		if err != nil {
			fmt.Fprintf(errOut, "Server error: %v\n", err)
			return 1
		}
		return 0
	}
}

func runList(ctx context.Context, args []string, out io.Writer, errOut io.Writer) int {
	fs := flag.NewFlagSet("a2a-mailbox list", flag.ContinueOnError)
	fs.SetOutput(errOut)
	backendType := fs.String("backend", "db", "Storage backend: db or tk")
	dbPath := fs.String("db", "", "Path to local mailbox JSON database")
	tkDir := fs.String("tk-dir", "", "Working directory for tk tickets")
	tkBin := fs.String("tk-bin", "tk", "Path to tk binary")
	contextID := fs.String("context", "", "Filter by contextId")
	status := fs.String("status", "", "Filter by task state")

	if err := fs.Parse(args); err != nil {
		return 2
	}

	b, err := openBackend(*backendType, *dbPath, *tkDir, *tkBin, "", "", "", "", "")
	if err != nil {
		fmt.Fprintf(errOut, "Error: %v\n", err)
		return 1
	}

	stateFilter := ""
	if *status != "" {
		stateFilter = normalizeState(*status)
	}
	tasks, err := b.ListTasks(ctx, *contextID, stateFilter)
	if err != nil {
		fmt.Fprintf(errOut, "Error listing tasks: %v\n", err)
		return 1
	}

	if len(tasks) == 0 {
		fmt.Fprintln(out, "No tasks in mailbox.")
		return 0
	}

	for _, t := range tasks {
		lastText := ""
		if len(t.History) > 0 && len(t.History[len(t.History)-1].Parts) > 0 {
			lastText = t.History[len(t.History)-1].Parts[0].Text
		}
		ticketInfo := ""
		if tid, ok := t.Metadata["tkTicketId"].(string); ok && tid != "" {
			ticketInfo = fmt.Sprintf("  [ticket: %s]", tid)
		}
		fmt.Fprintf(out, "%s  context=%s  state=%s%s  %q\n", t.ID, t.ContextID, t.Status.State, ticketInfo, lastText)
	}
	return 0
}

func runShow(ctx context.Context, args []string, out io.Writer, errOut io.Writer) int {
	fs := flag.NewFlagSet("a2a-mailbox show", flag.ContinueOnError)
	fs.SetOutput(errOut)
	backendType := fs.String("backend", "db", "Storage backend: db or tk")
	dbPath := fs.String("db", "", "Path to local mailbox JSON database")
	tkDir := fs.String("tk-dir", "", "Working directory for tk tickets")
	tkBin := fs.String("tk-bin", "tk", "Path to tk binary")

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() < 1 {
		fmt.Fprintln(errOut, "Usage: a2a-mailbox show [flags] <task-id>")
		return 2
	}
	taskID := fs.Arg(0)

	b, err := openBackend(*backendType, *dbPath, *tkDir, *tkBin, "", "", "", "", "")
	if err != nil {
		fmt.Fprintf(errOut, "Error: %v\n", err)
		return 1
	}

	task, err := b.GetTask(ctx, taskID)
	if err != nil {
		fmt.Fprintf(errOut, "Error: %v\n", err)
		return 1
	}

	data, _ := json.MarshalIndent(task, "", "  ")
	fmt.Fprintln(out, string(data))
	return 0
}

func runReply(ctx context.Context, args []string, out io.Writer, errOut io.Writer) int {
	fs := flag.NewFlagSet("a2a-mailbox reply", flag.ContinueOnError)
	fs.SetOutput(errOut)
	backendType := fs.String("backend", "db", "Storage backend: db or tk")
	dbPath := fs.String("db", "", "Path to local mailbox JSON database")
	tkDir := fs.String("tk-dir", "", "Working directory for tk tickets")
	tkBin := fs.String("tk-bin", "tk", "Path to tk binary")
	state := fs.String("state", "completed", "New task state (completed, input-required, working)")

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() < 2 {
		fmt.Fprintln(errOut, "Usage: a2a-mailbox reply [flags] <task-id> <reply-text>")
		return 2
	}
	taskID := fs.Arg(0)
	replyText := strings.Join(fs.Args()[1:], " ")

	b, err := openBackend(*backendType, *dbPath, *tkDir, *tkBin, "", "", "", "", "")
	if err != nil {
		fmt.Fprintf(errOut, "Error: %v\n", err)
		return 1
	}

	updated, err := b.ReplyTask(ctx, taskID, replyText, normalizeState(*state))
	if err != nil {
		fmt.Fprintf(errOut, "Error replying to task: %v\n", err)
		return 1
	}

	fmt.Fprintf(out, "Updated task %s (context=%s, state=%s)\n", updated.ID, updated.ContextID, updated.Status.State)
	return 0
}

func printUsage(out io.Writer) {
	fmt.Fprintln(out, `a2a-mailbox - Executor-less A2A v1.0 inbox server & CLI plugin

Usage:
  a2a-mailbox serve [--port 8085] [--backend db|tk|pubsub] [--db path] [--tk-dir dir]
  a2a-mailbox list  [--backend db|tk] [--db path] [--context id] [--status state]
  a2a-mailbox show  [--backend db|tk] [--db path] <task-id>
  a2a-mailbox reply [--backend db|tk] [--db path] [--state completed|input-required] <task-id> <text>
  a2a-mailbox info
  a2a-mailbox --version`)
}
