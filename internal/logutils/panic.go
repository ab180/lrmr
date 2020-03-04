package logutils

import (
	"bytes"
	"fmt"
	"github.com/maruel/panicparse/stack"
	"github.com/thoas/go-funk"
	"log"
	"os"
	"runtime"
	"strings"
)

type PanicError struct {
	Reason string
	Stack  string

	GoroutineBuckets []*stack.Bucket
}

func (pe PanicError) Error() string {
	return pe.Reason
}

func (pe PanicError) Pretty() string {
	return fmt.Sprintf("%s\n\n%s", pe.Reason, pe.Stack)
}

func WrapRecover(r interface{}) *PanicError {
	if r == nil {
		return nil
	}
	reason := fmt.Sprintf("panic: %s", r)

	st := make([]byte, 1024)
	for {
		n := runtime.Stack(st, true)
		if n < len(st) {
			st = st[:n]
			break
		}
		st = make([]byte, 2*len(st))
	}
	c, err := stack.ParseDump(bytes.NewReader(st), os.Stdout, true)
	if err != nil {
		log.Printf("warning: unable to parse panic stacktrace: %v\n", err)
		return &PanicError{
			Reason: reason,
			Stack:  string(st),
		}
	}

	// Find out similar goroutine traces and group them into buckets.
	buckets := stack.Aggregate(c.Goroutines, stack.AnyValue)

	// Calculate alignment.
	srcLen := 0
	for _, bucket := range buckets {
		for _, line := range bucket.Signature.Stack.Calls {
			if l := len(line.SrcLine()); l > srcLen {
				srcLen = l
			}
		}
	}

	prettyStack := ""
	for i, bucket := range buckets {
		index, _ := funk.FindKey(bucket.Stack.Calls, func(line stack.Call) bool {
			return line.Func.Name() == "panic"
		})
		if i == 0 && index != nil {
			panicIndex := index.(int)

			// remove stacks before main panic
			bucket.Stack.Calls = bucket.Stack.Calls[panicIndex+1:]
		} else if i != 0 {
			// dim text color
			prettyStack += "\n\x1b[2m"
		}

		// Print the goroutine header.
		extra := ""
		if s := bucket.SleepString(); s != "" {
			extra += "[" + s + "]"
		}
		if bucket.Locked {
			extra += "[locked]"
		}
		if c := bucket.CreatedByString(false); c != "" {
			extra += "[created by " + c + "]"
		}
		goroutineIDs := ""
		if len(bucket.IDs) < 3 {
			var ids []string
			for _, id := range bucket.IDs {
				ids = append(ids, fmt.Sprintf("#%d", id))
			}
			goroutineIDs = strings.Join(ids, ", ")
		} else {
			goroutineIDs = fmt.Sprintf("Group of %d goroutines", len(bucket.IDs))
		}

		prettyStack += fmt.Sprintf("%s: %s %s\n", goroutineIDs, bucket.State, extra)

		// Print the stack lines.
		for _, line := range bucket.Stack.Calls {
			prettyStack += fmt.Sprintf(
				"    %-*s  %s(%s)\n",
				srcLen, line.SrcLine(),
				line.Func.PkgDotName(), &line.Args)
		}
		if bucket.Stack.Elided {
			prettyStack += "    (...)\n"
		}

		if i != 0 {
			// reset text color
			prettyStack += "\x1b[0m"
		}
	}
	return &PanicError{
		Reason:           reason,
		Stack:            prettyStack,
		GoroutineBuckets: buckets,
	}
}
