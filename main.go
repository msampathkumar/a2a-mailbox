package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/msampathkumar/a2a-mailbox/internal/hooks"
	"github.com/msampathkumar/a2a-mailbox/internal/server"
	"github.com/msampathkumar/a2a-mailbox/internal/store"
)

const (
	version                = "0.1.0"
	maxAttachmentSizeBytes = 4 << 20 // 4 MiB
)

type pluginInfo struct {
	Name        string `json:"name"`
	Version     string `json:"version"`
	Description string `json:"description"`
}

type stringSliceFlag []string

func (s *stringSliceFlag) String() string { return strings.Join(*s, ",") }
func (s *stringSliceFlag) Set(val string) error {
	*s = append(*s, val)
	return nil
}

// commonFlags holds shared backend, `.env`, and lifecycle hook flags across subcommands.
type commonFlags struct {
	envFile      *string
	backendType  *string
	dbPath       *string
	tkDir        *string
	tkBin        *string
	beforeArrive *string
	afterArrive  *string
	beforeRead   *string
	afterRead    *string
	beforeUpdate *string
	afterUpdate  *string
}

func registerCommonFlags(fs *flag.FlagSet) *commonFlags {
	return &commonFlags{
		envFile:      fs.String("env-file", ".env", "Path to .env configuration file"),
		backendType:  fs.String("backend", "", "Storage backend: db, tk, or pubsub (overrides A2A_MAILBOX_BACKEND)"),
		dbPath:       fs.String("db", "", "Path to local mailbox JSON database (overrides A2A_MAILBOX_DB_PATH)"),
		tkDir:        fs.String("tk-dir", "", "Working directory for tk tickets (overrides A2A_MAILBOX_TK_DIR)"),
		tkBin:        fs.String("tk-bin", "tk", "Path to tk binary"),
		beforeArrive: fs.String("hook-before-arrive", "", "Command executed before incoming mail is registered"),
		afterArrive:  fs.String("hook-after-arrive", "", "Command executed after incoming mail is registered"),
		beforeRead:   fs.String("hook-before-read", "", "Command executed before mail is read"),
		afterRead:    fs.String("hook-after-read", "", "Command executed after mail is read"),
		beforeUpdate: fs.String("hook-before-update", "", "Command executed before task update/reply"),
		afterUpdate:  fs.String("hook-after-update", "", "Command executed after task update/reply"),
	}
}

func resolveVal(flagVal, envKey string, dotEnv map[string]string, fallback string) string {
	if strings.TrimSpace(flagVal) != "" {
		return strings.TrimSpace(flagVal)
	}
	if v, ok := dotEnv[envKey]; ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	if v := strings.TrimSpace(os.Getenv(envKey)); v != "" {
		return v
	}
	return fallback
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
				Description: "Executor-less A2A inbox server, lifecycle hooks & async responder backed by local DB, tk tickets, or Pub/Sub",
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
		case "claim", "promote":
			return runClaim(ctx, args[1:], out, errOut)
		case "reply", "update":
			return runReply(ctx, args[1:], out, errOut)
		case "work":
			return runWork(ctx, args[1:], out, errOut)
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

func openConfiguredBackend(
	cf *commonFlags,
	dotEnv map[string]string,
	pubsubEndpoint, pubsubProject, pubsubTopic, pubsubToken, ackState string,
	out io.Writer,
) (store.Backend, error) {
	backendType := resolveVal(*cf.backendType, "A2A_MAILBOX_BACKEND", dotEnv, "db")
	dbPath := resolveVal(*cf.dbPath, "A2A_MAILBOX_DB_PATH", dotEnv, defaultDBPath())
	tkDir := resolveVal(*cf.tkDir, "A2A_MAILBOX_TK_DIR", dotEnv, "")
	state := normalizeState(ackState)

	db, err := store.NewDBBackend(dbPath, state)
	if err != nil {
		return nil, err
	}

	var base store.Backend
	switch strings.ToLower(strings.TrimSpace(backendType)) {
	case "", "db":
		base = db
	case "tk":
		if tkDir == "" {
			cwd, err := os.Getwd()
			if err != nil {
				return nil, err
			}
			tkDir = cwd
		}
		base, err = store.NewTKBackend(db, *cf.tkBin, tkDir)
		if err != nil {
			return nil, err
		}
	case "pubsub":
		base, err = store.NewPubSubBackend(db, pubsubEndpoint, pubsubProject, pubsubTopic, pubsubToken)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown backend %q (choose db, tk, or pubsub)", backendType)
	}

	hcfg := hooks.Config{
		BeforeArrive: resolveVal(*cf.beforeArrive, "A2A_HOOK_BEFORE_ARRIVE", dotEnv, ""),
		AfterArrive:  resolveVal(*cf.afterArrive, "A2A_HOOK_AFTER_ARRIVE", dotEnv, ""),
		BeforeRead:   resolveVal(*cf.beforeRead, "A2A_HOOK_BEFORE_READ", dotEnv, ""),
		AfterRead:    resolveVal(*cf.afterRead, "A2A_HOOK_AFTER_READ", dotEnv, ""),
		BeforeUpdate: resolveVal(*cf.beforeUpdate, "A2A_HOOK_BEFORE_UPDATE", dotEnv, ""),
		AfterUpdate:  resolveVal(*cf.afterUpdate, "A2A_HOOK_AFTER_UPDATE", dotEnv, ""),
		Out:          out,
		ExtraEnv:     dotEnv,
	}
	if hcfg.HasAny() {
		return hooks.NewBackend(base, hcfg), nil
	}
	return base, nil
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

func loadFileAttachments(filePaths, fileURLs []string) ([]store.FileAttachment, error) {
	var attachments []store.FileAttachment
	for _, p := range filePaths {
		clean := filepath.Clean(strings.TrimSpace(p))
		info, err := os.Stat(clean)
		if err != nil {
			return nil, fmt.Errorf("stat attachment %q: %w", clean, err)
		}
		if info.IsDir() {
			return nil, fmt.Errorf("attachment %q is a directory", clean)
		}
		if info.Size() > maxAttachmentSizeBytes {
			return nil, fmt.Errorf("attachment %q exceeds 4 MiB limit", clean)
		}
		data, err := os.ReadFile(clean)
		if err != nil {
			return nil, fmt.Errorf("reading attachment %q: %w", clean, err)
		}
		mtype := mime.TypeByExtension(filepath.Ext(clean))
		if mtype == "" {
			mtype = http.DetectContentType(data)
		}
		attachments = append(attachments, store.FileAttachment{
			Filename:  filepath.Base(clean),
			MediaType: mtype,
			Raw:       base64.StdEncoding.EncodeToString(data),
		})
	}
	for _, u := range fileURLs {
		trimmed := strings.TrimSpace(u)
		if trimmed == "" {
			continue
		}
		attachments = append(attachments, store.FileAttachment{
			Filename: filepath.Base(trimmed),
			URL:      trimmed,
		})
	}
	return attachments, nil
}

func runServe(ctx context.Context, args []string, out io.Writer, errOut io.Writer) int {
	fs := flag.NewFlagSet("a2a-mailbox serve", flag.ContinueOnError)
	fs.SetOutput(errOut)
	cf := registerCommonFlags(fs)
	host := fs.String("host", "", "Bind host address (default 127.0.0.1)")
	port := fs.Int("port", 0, "Listen port (default 8085)")
	pubsubEndpoint := fs.String("pubsub-endpoint", "", "Google Cloud Pub/Sub REST endpoint override")
	pubsubProject := fs.String("pubsub-project", os.Getenv("GOOGLE_CLOUD_PROJECT"), "Google Cloud project ID for Pub/Sub")
	pubsubTopic := fs.String("pubsub-topic", "a2a-mailbox", "Google Cloud Pub/Sub topic ID")
	ackState := fs.String("ack-state", "", "Initial A2A task state on message registration")
	responseMode := fs.String("response-mode", "", "Response envelope mode: task (default) or message")
	name := fs.String("name", "", "Agent Card name")
	desc := fs.String("description", "Executor-less A2A inbox server that registers and stores incoming tasks", "Agent Card description")

	if err := fs.Parse(args); err != nil {
		return 2
	}

	dotEnv, _ := hooks.LoadDotEnv(*cf.envFile)
	resolvedHost := resolveVal(*host, "A2A_MAILBOX_HOST", dotEnv, "127.0.0.1")
	resolvedPort := *port
	if resolvedPort == 0 {
		if pStr := resolveVal("", "A2A_MAILBOX_PORT", dotEnv, "8085"); pStr != "" {
			if p, err := strconv.Atoi(pStr); err == nil && p > 0 {
				resolvedPort = p
			}
		}
		if resolvedPort == 0 {
			resolvedPort = 8085
		}
	}
	resolvedAck := resolveVal(*ackState, "A2A_MAILBOX_ACK_STATE", dotEnv, "input-required")
	resolvedMode := resolveVal(*responseMode, "A2A_MAILBOX_RESPONSE_MODE", dotEnv, "task")
	resolvedName := resolveVal(*name, "A2A_MAILBOX_NAME", dotEnv, "A2A Mailbox")

	b, err := openConfiguredBackend(
		cf,
		dotEnv,
		*pubsubEndpoint,
		*pubsubProject,
		*pubsubTopic,
		os.Getenv("PUBSUB_BEARER_TOKEN"),
		resolvedAck,
		out,
	)
	if err != nil {
		fmt.Fprintf(errOut, "Error initializing backend: %v\n", err)
		return 1
	}

	addr := fmt.Sprintf("%s:%d", resolvedHost, resolvedPort)
	baseURL := fmt.Sprintf("http://%s", addr)
	handler := server.NewHandler(server.Config{
		Name:         resolvedName,
		Description:  *desc,
		Version:      version,
		BaseURL:      baseURL,
		ResponseMode: resolvedMode,
		Backend:      b,
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		fmt.Fprintf(out, "A2A Mailbox listening on %s (backend=%s, card=%s/.well-known/agent-card.json)\n", baseURL, b.Name(), baseURL)
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
	cf := registerCommonFlags(fs)
	contextID := fs.String("context", "", "Filter by contextId")
	status := fs.String("status", "", "Filter by task state")

	if err := fs.Parse(args); err != nil {
		return 2
	}

	dotEnv, _ := hooks.LoadDotEnv(*cf.envFile)
	b, err := openConfiguredBackend(cf, dotEnv, "", "", "", "", "", out)
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
		kind, _ := t.Metadata["kind"].(string)
		if kind == "" {
			kind = "task"
		}
		ticketInfo := ""
		if tid, ok := t.Metadata["tkTicketId"].(string); ok && tid != "" {
			ticketInfo = fmt.Sprintf("  [ticket: %s]", tid)
		}
		fmt.Fprintf(out, "%s  kind=%s  context=%s  state=%s%s  %q\n", t.ID, kind, t.ContextID, t.Status.State, ticketInfo, lastText)
	}
	return 0
}

func runShow(ctx context.Context, args []string, out io.Writer, errOut io.Writer) int {
	fs := flag.NewFlagSet("a2a-mailbox show", flag.ContinueOnError)
	fs.SetOutput(errOut)
	cf := registerCommonFlags(fs)
	saveFilesDir := fs.String("save-files", "", "Optional directory to extract attached file parts into")

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() < 1 {
		fmt.Fprintln(errOut, "Usage: a2a-mailbox show [flags] <task-id>")
		return 2
	}
	taskID := fs.Arg(0)

	dotEnv, _ := hooks.LoadDotEnv(*cf.envFile)
	b, err := openConfiguredBackend(cf, dotEnv, "", "", "", "", "", out)
	if err != nil {
		fmt.Fprintf(errOut, "Error: %v\n", err)
		return 1
	}

	task, err := b.GetTask(ctx, taskID)
	if err != nil {
		fmt.Fprintf(errOut, "Error: %v\n", err)
		return 1
	}

	if strings.TrimSpace(*saveFilesDir) != "" {
		saved, err := store.SaveTaskFiles(task, *saveFilesDir)
		if err != nil {
			fmt.Fprintf(errOut, "Error saving file parts: %v\n", err)
			return 1
		}
		for _, p := range saved {
			fmt.Fprintf(errOut, "Saved file part: %s\n", p)
		}
	}

	data, _ := json.MarshalIndent(task, "", "  ")
	fmt.Fprintln(out, string(data))
	return 0
}

func runClaim(ctx context.Context, args []string, out io.Writer, errOut io.Writer) int {
	fs := flag.NewFlagSet("a2a-mailbox claim", flag.ContinueOnError)
	fs.SetOutput(errOut)
	cf := registerCommonFlags(fs)
	workerID := fs.String("worker", "", "Responder/worker identifier")

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() < 1 {
		fmt.Fprintln(errOut, "Usage: a2a-mailbox claim [flags] <task-id> [status-message]")
		return 2
	}
	dotEnv, _ := hooks.LoadDotEnv(*cf.envFile)
	resolvedWorker := resolveVal(*workerID, "A2A_WORKER_ID", dotEnv, "mailbox-responder")

	taskID := fs.Arg(0)
	msg := "Task claimed by responder " + resolvedWorker
	if fs.NArg() > 1 {
		msg = strings.Join(fs.Args()[1:], " ")
	}

	b, err := openConfiguredBackend(cf, dotEnv, "", "", "", "", "", out)
	if err != nil {
		fmt.Fprintf(errOut, "Error: %v\n", err)
		return 1
	}

	updated, err := b.RespondTask(ctx, store.RespondRequest{
		TaskID:   taskID,
		State:    "TASK_STATE_WORKING",
		WorkerID: resolvedWorker,
		Text:     msg,
	})
	if err != nil {
		fmt.Fprintf(errOut, "Error claiming task: %v\n", err)
		return 1
	}

	fmt.Fprintf(out, "Claimed task %s (context=%s, state=%s, worker=%s)\n",
		updated.ID, updated.ContextID, updated.Status.State, resolvedWorker)
	return 0
}

func runReply(ctx context.Context, args []string, out io.Writer, errOut io.Writer) int {
	fs := flag.NewFlagSet("a2a-mailbox reply", flag.ContinueOnError)
	fs.SetOutput(errOut)
	cf := registerCommonFlags(fs)
	state := fs.String("state", "completed", "New task state (working, input-required, completed, failed)")
	workerID := fs.String("worker", "", "Optional responder/worker ID")
	var files, fileURLs stringSliceFlag
	fs.Var(&files, "file", "Attach a local file to the response/artifacts (repeatable)")
	fs.Var(&fileURLs, "file-url", "Attach a file URL to the response/artifacts (repeatable)")

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() < 2 {
		fmt.Fprintln(errOut, "Usage: a2a-mailbox reply [flags] <task-id> <reply-text>")
		return 2
	}
	taskID := fs.Arg(0)
	replyText := strings.Join(fs.Args()[1:], " ")

	attachments, err := loadFileAttachments(files, fileURLs)
	if err != nil {
		fmt.Fprintf(errOut, "Error loading file attachments: %v\n", err)
		return 1
	}

	dotEnv, _ := hooks.LoadDotEnv(*cf.envFile)
	resolvedWorker := resolveVal(*workerID, "A2A_WORKER_ID", dotEnv, "")

	b, err := openConfiguredBackend(cf, dotEnv, "", "", "", "", "", out)
	if err != nil {
		fmt.Fprintf(errOut, "Error: %v\n", err)
		return 1
	}

	updated, err := b.RespondTask(ctx, store.RespondRequest{
		TaskID:   taskID,
		Text:     replyText,
		State:    normalizeState(*state),
		WorkerID: resolvedWorker,
		Files:    attachments,
	})
	if err != nil {
		fmt.Fprintf(errOut, "Error replying to task: %v\n", err)
		return 1
	}

	fmt.Fprintf(out, "Updated task %s (context=%s, state=%s, files=%d)\n",
		updated.ID, updated.ContextID, updated.Status.State, len(attachments))
	return 0
}

func runWork(ctx context.Context, args []string, out io.Writer, errOut io.Writer) int {
	fs := flag.NewFlagSet("a2a-mailbox work", flag.ContinueOnError)
	fs.SetOutput(errOut)
	cf := registerCommonFlags(fs)
	workerID := fs.String("worker", "", "Worker identifier (overrides A2A_WORKER_ID)")
	once := fs.Bool("once", false, "Process current actionable tasks once and exit")
	interval := fs.Duration("interval", 2*time.Second, "Polling interval when running continuously")
	state := fs.String("state", "", "Target task state after processing (overrides A2A_WORKER_STATE)")
	replyText := fs.String("reply", "", "Static or template reply message when --exec is not used")
	execBin := fs.String("exec", "", "Optional handler command to invoke for each task (overrides A2A_WORKER_EXEC)")
	var files, fileURLs stringSliceFlag
	fs.Var(&files, "file", "Attach a local file to processed tasks (repeatable)")
	fs.Var(&fileURLs, "file-url", "Attach a file URL to processed tasks (repeatable)")

	if err := fs.Parse(args); err != nil {
		return 2
	}

	attachments, err := loadFileAttachments(files, fileURLs)
	if err != nil {
		fmt.Fprintf(errOut, "Error loading file attachments: %v\n", err)
		return 1
	}

	dotEnv, _ := hooks.LoadDotEnv(*cf.envFile)
	resolvedWorker := resolveVal(*workerID, "A2A_WORKER_ID", dotEnv, "mailbox-worker")
	resolvedState := normalizeState(resolveVal(*state, "A2A_WORKER_STATE", dotEnv, "completed"))
	resolvedExec := resolveVal(*execBin, "A2A_WORKER_EXEC", dotEnv, "")

	b, err := openConfiguredBackend(cf, dotEnv, "", "", "", "", "", out)
	if err != nil {
		fmt.Fprintf(errOut, "Error: %v\n", err)
		return 1
	}

	for {
		processed, err := processActionableTasks(ctx, b, resolvedWorker, resolvedState, *replyText, resolvedExec, attachments, out, errOut)
		if err != nil {
			fmt.Fprintf(errOut, "Worker error: %v\n", err)
			return 1
		}
		if *once {
			fmt.Fprintf(out, "Worker completed pass (processed=%d)\n", processed)
			return 0
		}
		select {
		case <-ctx.Done():
			return 0
		case <-time.After(*interval):
		}
	}
}

func processActionableTasks(
	ctx context.Context,
	b store.Backend,
	workerID, targetState, staticReply, execCmd string,
	staticFiles []store.FileAttachment,
	out, errOut io.Writer,
) (int, error) {
	tasks, err := b.ListTasks(ctx, "", "")
	if err != nil {
		return 0, err
	}

	count := 0
	for _, t := range tasks {
		if store.IsTerminalProtocolState(t.Status.State) {
			continue
		}
		awaiting, ok := t.Metadata["awaitingWorker"].(bool)
		if ok && !awaiting {
			continue
		}

		// Claim the task as WORKING first.
		_, _ = b.RespondTask(ctx, store.RespondRequest{
			TaskID:   t.ID,
			State:    "TASK_STATE_WORKING",
			WorkerID: workerID,
			Text:     "Worker " + workerID + " processing task...",
		})

		req := store.RespondRequest{
			TaskID:   t.ID,
			State:    targetState,
			WorkerID: workerID,
			Text:     staticReply,
			Files:    staticFiles,
		}

		if strings.TrimSpace(execCmd) != "" {
			execReq, err := invokeWorkerExec(ctx, execCmd, t, workerID, targetState, out)
			if err != nil {
				fmt.Fprintf(errOut, "Handler error for task %s: %v\n", t.ID, err)
				continue
			}
			req = execReq
			req.Files = append(req.Files, staticFiles...)
		} else if strings.TrimSpace(req.Text) == "" {
			req.Text = fmt.Sprintf("Processed by worker %s", workerID)
		}

		updated, err := b.RespondTask(ctx, req)
		if err != nil {
			return count, err
		}
		fmt.Fprintf(out, "Processed task %s -> %s\n", updated.ID, updated.Status.State)
		count++
	}
	return count, nil
}

func invokeWorkerExec(ctx context.Context, execCmd string, t *store.Task, workerID, defaultState string, out io.Writer) (store.RespondRequest, error) {
	trimmed := strings.TrimSpace(execCmd)
	if strings.ContainsAny(trimmed, ";|&$`<>") {
		return store.RespondRequest{}, fmt.Errorf("invalid characters in worker executable %q", trimmed)
	}
	fields := strings.Fields(trimmed)
	if len(fields) == 0 {
		return store.RespondRequest{}, errors.New("empty worker command")
	}
	resolved, err := exec.LookPath(fields[0])
	if err != nil {
		return store.RespondRequest{}, fmt.Errorf("resolving worker executable %q: %w", fields[0], err)
	}

	lastText := ""
	for i := len(t.History) - 1; i >= 0; i-- {
		if t.History[i].Role == "ROLE_USER" && len(t.History[i].Parts) > 0 {
			lastText = t.History[i].Parts[0].Text
			break
		}
	}
	if lastText == "" && len(t.History) > 0 && len(t.History[len(t.History)-1].Parts) > 0 {
		lastText = t.History[len(t.History)-1].Parts[0].Text
	}
	taskJSON, _ := json.Marshal(t)
	payloadBytes, _ := json.Marshal(map[string]any{
		"event":     "worker_exec",
		"taskId":    t.ID,
		"contextId": t.ContextID,
		"text":      lastText,
		"task":      t,
	})

	cmd := exec.CommandContext(ctx, filepath.Clean(resolved), fields[1:]...)
	cmd.Stdin = bytes.NewReader(payloadBytes)
	cmd.Env = append(os.Environ(),
		"A2A_TASK_ID="+t.ID,
		"A2A_CONTEXT_ID="+t.ContextID,
		"A2A_MESSAGE_TEXT="+lastText,
		"A2A_TASK_JSON="+string(taskJSON),
	)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return store.RespondRequest{}, fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}

	rawOut := stdout.String()
	var structured *store.RespondRequest
	var displayLines []string

	for _, line := range strings.Split(rawOut, "\n") {
		trimmedLine := strings.TrimSpace(line)
		if strings.HasPrefix(trimmedLine, "A2A_HOOK_RESULT:") {
			jsonPart := strings.TrimSpace(strings.TrimPrefix(trimmedLine, "A2A_HOOK_RESULT:"))
			var parsed store.RespondRequest
			if json.Unmarshal([]byte(jsonPart), &parsed) == nil {
				structured = &parsed
				continue
			}
		}
		var parsed store.RespondRequest
		if strings.HasPrefix(trimmedLine, "{") && json.Unmarshal([]byte(trimmedLine), &parsed) == nil &&
			(parsed.Text != "" || parsed.State != "" || len(parsed.Files) > 0) {
			structured = &parsed
			continue
		}
		displayLines = append(displayLines, line)
	}

	displayText := strings.Join(displayLines, "\n")
	if strings.TrimSpace(displayText) != "" && out != nil {
		_, _ = io.WriteString(out, displayText)
		if !strings.HasSuffix(displayText, "\n") {
			_, _ = io.WriteString(out, "\n")
		}
	}

	if structured != nil {
		structured.TaskID = t.ID
		if structured.WorkerID == "" {
			structured.WorkerID = workerID
		}
		if structured.State == "" {
			structured.State = defaultState
		} else {
			structured.State = normalizeState(structured.State)
		}
		return *structured, nil
	}

	return store.RespondRequest{
		TaskID:   t.ID,
		State:    defaultState,
		WorkerID: workerID,
		Text:     strings.TrimSpace(rawOut),
	}, nil
}

func printUsage(out io.Writer) {
	fmt.Fprintln(out, `a2a-mailbox - Standalone Executor-less A2A v1.0 Inbox Server, Lifecycle Hooks & Async Responder

Usage:
  a2a-mailbox serve [--env-file .env] [--port 8085] [--backend db|tk|pubsub] [--db path] [--tk-dir dir]
  a2a-mailbox list  [--env-file .env] [--backend db|tk] [--db path] [--context id] [--status state]
  a2a-mailbox show  [--env-file .env] [--backend db|tk] [--db path] <task-id>
  a2a-mailbox claim [--env-file .env] [--backend db|tk] [--db path] [--worker id] <task-id> [message]
  a2a-mailbox reply [--env-file .env] [--backend db|tk] [--db path] [--state working|input-required|completed] [--file path] <task-id> <text>
  a2a-mailbox work  [--env-file .env] [--backend db|tk] [--db path] [--once] [--worker id] [--exec cmd] [--file path] [--reply text]
  a2a-mailbox info
  a2a-mailbox --version`)
}
