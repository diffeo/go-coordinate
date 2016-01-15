Go Coordinate Worker
====================

The worker library provides a generic system to use the Coordinate
system as a job queue for Go-based tasks.

Work Specs
----------

Work specs should be set up with two special keys that control the
worker.  A work spec must contain a key `runtime: go` for the worker
to attempt it.  The work spec should also contain a key `task` that
names one of the tasks passed to the worker library; if it does not,
the name of the work spec is used as the task name.

```yaml
flows:
  # This runs a Go-based task named "hello"
  hello:
    min_gb: 1
    runtime: go

  # This also runs "hello"
  greeting:
    min_gb: 1
    runtime: go
    task: hello
    
  # This runs in the Python coordinate_worker and the Go worker ignores it
  coordinate_test_spec:
    min_gb: 1
    module: coordinate.test.test_job_client
    run_function: run_function
```

Tasks
-----

A task is defined by a Go function that takes a context with
cancellation information, and a set of Coordinate work unit attempts.
Unless the implementation has strong reason to believe that its code
will finish very quickly, it must periodically check the context
`Done()` channel to see if the job has been cancelled; in
computationally intensive code, try to check this about once per
second.

The attempts passed to the task are guaranteed to be for distinct work
units, all for the same work spec, and all with the same worker, and
there is guaranteed to be at least one.  The implementation can use
any strategy to complete these, including running them serially,
running them one per goroutine, or feeding them into a batch process
that will eventually generate a single output.  Note that the worker
library itself will run several tasks in concurrent goroutines, so a
serial execution may work fine.

The task implementation is responsible for setting the final fate of
the attempts.  If it does not, they will eventually time out and be
retried.  The task implementation should recover from panics and fail
the affected attempt(s).

```go
import (
        "fmt"
        "github.com/diffeo/go-coordinate/coordinate"
        "golang.org/x/net/context"
        "runtime"
)

func PrintKeyTask(ctx context.Context, attempts []coordinate.Attempt) {
        i := 0
        defer func() {
                if i < len(attempts) {
                        // Fail any remaining attempts.
                        var data map[string]interface{}
                        // Did we panic?
                        if obj := recover(); obj != nil {
                                data = make(map[string]interface{})
                                msg := fmt.Sprintf("%+v", obj)
                                stack := make([]byte, 4096)
                                count := runtime.Stack(stack, false)
                                traceback := string(stack[0:count])
                                data["panic"] = msg
                                data["stack"] = traceback
                                data["traceback"] = msg + "\n" + traceback
                        }
                        // Fail all remaining attempts, ignoring errors
                        for ; i < len(attempts); i++ {
                                _ = attempts[i].Fail(data)
                        }
                }
        }()
        for ; i < len(attempts); i++ {
                // Stop now if cancellation was requested
                select {
                case <-ctx.Done():
                        panic(ctx.Err())
                }
                // Do the actual work
                fmt.Printf("Doing work unit %v\n",
                        attempts[i].WorkUnit().Name())
                // Clean up
                err := attempts[i].Finish(nil)
                if err != nil {
                        panic(err)
                }
        }
}
```

Expiration
----------

The expiration mechanism actively depends on cooperation by task
implementations.

The passed context will not have a set deadline, but the worker
library will cancel it 30 seconds before the expiration time of the
first attempt returned.  (If a work unit requests a very short
lifetime this could cause it to be cancelled immediately.)  This will
cause the context's `Done()` channel to be closed and return
`context.Canceled` as its error.  The task implementation can perform
any reasonable activity in response to this, including marking
incomplete attempts as expired so they can be retried later.

If the task function has not returned within 15 seconds of the first
attempt's expiration time, or within the last quarter of the attempt
lifetime for short attempts, the worker framework will fail all of the
attempts that are still pending, and hope that the task function will
pick itself up soon.

Alternatives
------------

The expiration mechanism protects against two things: workers that
somehow fail unrecoverably without checking back in (e.g., hardware
failures) and runaway jobs.  Calling `attempt.Renew()` can support
very-long-running jobs, but in practice, a single job that wants to
run for hours will tie up a worker thread that probably could be doing
more useful work.

`context.WithDeadline()` allows setting a time at which point the
context library will automatically signal the context.  It is not
obvious how this interacts with mock time libraries, and will not work
with jobs that do try to renew themselves to get more time.

The Python worker runs workers in subprocesses, using `os.fork()`.
The setup for this is finicky, but the principal advantage of it is
that the parent worker can `os.kill()` the child when it runs over
time.  Practical experience also suggested ending worker subprocesses
before the Python garbage collector needed to run, a constraint that
hopefully is less significant for Go workers.  Go code cannot, in
isolation, **fork**(2); if this model were desirable, subprocesses
could be spawned with code like

```go
import "os"

// This executable path is Linux-specific and non-portable
child, err := os.StartProcess("/proc/self/exe", []string{"-dowork"}, nil)
...
// when we detect that child jobs are timing out
child.Kill()
```
