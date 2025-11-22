package internal

import (
	"fmt"
	"runtime/debug"
)

func TraceErr(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w\nStack:\n%s\n", err, debug.Stack())
}
