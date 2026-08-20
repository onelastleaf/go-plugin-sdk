# onelastleaf Go plugin SDK

This is the Go runtime for writing trusted [onelastleaf](https://github.com/onelastleaf/onelastleaf) process plugins. You register ordinary Go functions as actions; the SDK takes care of the long-lived gRPC connection, handshake, concurrent jobs, cancellation, host calls, heartbeat, and shutdown.

The module path is `github.com/onelastleaf/go-plugin-sdk`. It requires Go 1.24 or newer.

> [!NOTE]
> This repository is a library, not a standalone plugin. The quickest way to get a runnable project is `oll plugin new`, as shown below.

## Build and test the SDK

Clone the released SDK repository, then use the normal Go commands:

```sh
git clone https://github.com/onelastleaf/go-plugin-sdk.git
cd go-plugin-sdk
git checkout v0.1.0

go mod download
go test ./...
go test -race ./...
go build ./...
```

The generated protobuf files are committed, so normal builds do not require `protoc` or a gRPC code generator.

## Create a Go plugin

Let oll generate the small amount of project wiring for you:

```sh
oll plugin new hello-plugin \
  --language go \
  --id dev.example.hello \
  --name hello-plugin

cd hello-plugin
go mod download
go test ./...
go build -o ./hello-plugin ./cmd/plugin
```

The generated `go.mod` pulls the tagged SDK release from GitHub:

```go.mod
require github.com/onelastleaf/go-plugin-sdk v0.1.0
```

For a project you created by hand, add the same remote dependency with:

```sh
go get github.com/onelastleaf/go-plugin-sdk@v0.1.0
```

The generated project contains:

```text
hello-plugin/
├── cmd/plugin/main.go   # registers actions and starts the runtime
├── echo/echo.go         # example action
├── echo/echo_test.go
├── go.mod
└── oll.toml             # tells oll how to build and launch the plugin
```

Its entry point is deliberately small:

```go
package main

import (
	"context"
	"log"
	"strings"

	plugin "github.com/onelastleaf/go-plugin-sdk"
)

func echo(_ plugin.ActionContext, arguments []string) (plugin.ActionResult, error) {
	return plugin.StringResult(strings.Join(arguments, " ")), nil
}

func main() {
	runtime, err := plugin.New("dev.example.hello", "0.1.0").
		Action("echo", "Return the supplied arguments", echo).
		Build()
	if err != nil {
		log.Fatal(err)
	}
	if err := runtime.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
```

`plugin.New` takes the plugin's immutable ID and its version. Each call to `Action` adds a named action that oll can invoke. An action receives ordered string arguments and an `ActionContext`; return an `ActionResult` or an error.

Keep the ID in the binary identical to `plugin.id` in `oll.toml`. The generated project already does this.

## How oll starts the plugin

The generated `oll.toml` is both the build recipe and the runtime declaration. For the example above it looks like this:

```toml
format_version = 1

[plugin]
id = "dev.example.hello"
name = "hello-plugin"
protocol_fingerprint = "21fd97cf8ec1a89ef464192fe69d123469410c40910d3a7b74898224da61545a"

[source]
checkout = "source"
steps = [
  ["go", "build", "-o", "{install}/hello-plugin", "./cmd/plugin"],
]

[source.dependencies]
"go" = "Install Go and ensure go is in PATH."

[runtime]
argv = ["{install}/hello-plugin"]
```

When the plugin is started, oll:

1. opens a gRPC server on a kernel-selected loopback port;
2. launches the runtime command from `oll.toml`;
3. passes the server address in `OLL_PLUGIN_ENDPOINT`; and
4. keeps the plugin's stdin open as a parent-liveness pipe.

`runtime.Run(...)` connects to that endpoint as a gRPC client and stays connected until oll requests shutdown, the parent disappears, the context is cancelled, or the session fails. Do not use stdin for application input, and do not hard-code or expose a listening port in your plugin.

## Install, start, and call it

These commands assume the `oll` CLI is connected to a running onelastleaf node. oll installs source plugins from Git, so commit the generated project, push it to a remote repository, and give that remote to `plugin install`:

```sh
git init
git add .
git commit -m "Add hello plugin"
git branch -M main
git remote add origin https://github.com/your-org/hello-plugin.git
git push -u origin main

oll plugin install https://github.com/your-org/hello-plugin.git --source
oll plugin start dev.example.hello
oll plugin call dev.example.hello echo -- hello from Go
```

`plugin call` prints a job ID after the plugin accepts the work; it does not wait for the action's final value. Use that ID to inspect the result:

```sh
oll job info <job-id>
oll plugin log dev.example.hello
```

After changing the plugin, push the commit before asking oll to fetch the new source generation:

```sh
git add .
git commit -m "Update hello plugin"
git push
oll plugin update dev.example.hello
oll plugin restart dev.example.hello
```

An update does not restart a running plugin automatically. Stop it when you are done:

```sh
oll plugin stop dev.example.hello
```

## Inside an action

`ActionContext` is scoped to one job. Its most useful operations are:

- `Context()` for cancellation and deadlines;
- `JobID()` and `Trace()` for job and tracing metadata;
- `GetConfig(...)` and `InvokeConfigFunction(...)` for oll-owned plugin configuration;
- `HostCall(...)` for other permitted host capabilities; and
- `Log(...)` and `StoreArtifact(...)` for structured logs and verified artifacts.

Pass `action.Context()` to blocking work and return promptly when it is cancelled. A plugin may run several jobs at once, so avoid global mutable state unless you protect it yourself.

The SDK fills in the parent call ID and trace depth for nested host calls, while
job-scoped logs and artifact messages keep the job trace unchanged. It does not
expose a raw host client, so an action cannot accidentally construct either
trace incorrectly.

### Return a useful error

An ordinary Go error becomes an internal job failure. When the caller can act
on the error, return `ActionError` instead:

```go
return plugin.ActionResult{}, &plugin.ActionError{
    Code:     protocol.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
    Message:  "the input path is empty",
    Metadata: map[string]string{"argument": "path"},
}
```

Errors returned by oll can be matched with `errors.As` as `*plugin.HostError`.
Its `Code()`, `Retryable()`, and `Detail()` methods preserve the complete
structured error, including metadata and protobuf `Any` details.

### Store an artifact without loading it into memory

Give `StoreArtifact` an `io.ReadSeeker`. The SDK reads it twice: once to compute
the size and SHA-256, then again to stream bounded chunks. Include only the
descriptor returned by `StoreArtifact` in the action result:

```go
file, err := os.Open("report.json")
if err != nil {
    return plugin.ActionResult{}, err
}
defer file.Close()

artifact, err := action.StoreArtifact(plugin.ArtifactInput{
    ID:        "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    FileName:  "report.json",
    MediaType: "application/json",
    Source:    file,
})
if err != nil {
    return plugin.ActionResult{}, err
}
return plugin.ActionResult{Artifacts: []*protocol.ArtifactDescriptor{artifact}}, nil
```

Artifact IDs are canonical UUID v4 strings. File names must be safe base names,
and the source must still contain the same bytes on the second read.

For the packaging format, lifecycle, and CLI behavior, see the onelastleaf documentation for [plugin SDKs](https://github.com/onelastleaf/onelastleaf/blob/main/docs/plugin-sdk.md), [plugin packaging](https://github.com/onelastleaf/onelastleaf/blob/main/docs/plugin-packaging.md), and the [plugin system](https://github.com/onelastleaf/onelastleaf/blob/main/docs/plugin-system.md).

## Common surprises

- **`OLL_PLUGIN_ENDPOINT is required`** — the binary was started by hand. Start an installed plugin with `oll plugin start` instead.
- **Protocol fingerprint mismatch** — the SDK release, `oll.toml`, and oll binary do not describe the same protocol revision. Regenerate the project or update them together.
- **An update did not pick up local edits** — oll installs committed Git state, not uncommitted files. Commit the change before `oll plugin update`.
- **A call returned only a job ID** — that is normal. Read the eventual result with `oll job info <job-id>`.

## Updating the protocol bindings

Released plugins do not need protobuf tools. This section is only for SDK
maintainers updating the SDK alongside an oll protocol release.

Install exactly `protoc` 31.1, `protoc-gen-go` 1.36.9, and
`protoc-gen-go-grpc` 1.5.1. Then pass a clean onelastleaf checkout and the
fingerprint published by that oll build:

```sh
./scripts/update-protocol.sh \
  /path/to/onelastleaf \
  21fd97cf8ec1a89ef464192fe69d123469410c40910d3a7b74898224da61545a
```

The script verifies the published value against oll's complete canonical
descriptor set before it copies the four plugin protocol sources, regenerates
the Go bindings, and updates the SDK constant. Review and commit all of those
changes together.
