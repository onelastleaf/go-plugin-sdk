package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"

	plugin "github.com/onelastleaf/go-plugin-sdk"
	protocol "github.com/onelastleaf/go-plugin-sdk/protocol"
)

func main() {
	runtime, err := plugin.New("org.onelastleaf.conformance", "0.1.0").
		Action("echo", "Echo arguments", echo).
		Action("wait", "Wait for cancellation", wait).
		Action("host", "Exercise host capabilities", hostCalls).
		Action("artifact", "Exercise artifact transfer", artifact).
		Build()
	if err != nil {
		log.Fatal(err)
	}
	if err := runtime.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func echo(_ plugin.ActionContext, arguments []string) (plugin.ActionResult, error) {
	return plugin.StringResult(strings.Join(arguments, " ")), nil
}

func wait(action plugin.ActionContext, _ []string) (plugin.ActionResult, error) {
	<-action.Context().Done()
	return plugin.ActionResult{}, nil
}

func hostCalls(action plugin.ActionContext, _ []string) (plugin.ActionResult, error) {
	configured, err := action.GetConfig(nil)
	if err != nil {
		return plugin.ActionResult{}, err
	}
	function := configured.GetValue().GetFunctionValue()
	if function == nil {
		return plugin.ActionResult{}, fmt.Errorf("GetConfig omitted function")
	}
	invoked, err := action.InvokeConfigFunction(function, []*protocol.ConfigValue{stringValue("config")})
	if err != nil {
		return plugin.ActionResult{}, err
	}
	if len(invoked.Results) != 1 {
		return plugin.ActionResult{}, fmt.Errorf("function returned the wrong result count")
	}
	document, err := action.HostCall(&protocol.HostCallRequest{Call: &protocol.HostCallRequest_ReadDocument{
		ReadDocument: &protocol.ReadDocumentRequest{
			Path:       &protocol.DocumentPath{Value: "/conformance.md"},
			Projection: protocol.DocumentProjection_DOCUMENT_PROJECTION_CONTENT,
		},
	}})
	if err != nil {
		return plugin.ActionResult{}, err
	}
	content := document.GetReadDocument().GetDocument().GetContent()
	if err := action.Log(protocol.LogLevel_LOG_LEVEL_INFO, "conformance", "host action complete", nil); err != nil {
		return plugin.ActionResult{}, err
	}
	return plugin.StringResult(invoked.Results[0].GetStringValue() + "|" + content), nil
}

func artifact(action plugin.ActionContext, _ []string) (plugin.ActionResult, error) {
	descriptor, err := action.StoreArtifact(plugin.ArtifactInput{
		ID:        "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		FileName:  "conformance.txt",
		MediaType: "text/plain",
		Source:    bytes.NewReader([]byte("artifact payload")),
	})
	if err != nil {
		return plugin.ActionResult{}, err
	}
	return plugin.ActionResult{Result: stringValue("artifact"), Artifacts: []*protocol.ArtifactDescriptor{descriptor}}, nil
}

func stringValue(value string) *protocol.ConfigValue {
	return &protocol.ConfigValue{Kind: &protocol.ConfigValue_StringValue{StringValue: value}}
}
