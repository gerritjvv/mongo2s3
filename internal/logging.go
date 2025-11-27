package internal

import (
	"log/slog"
	"os"
)

var Log = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
	AddSource: true,
}))
