package pluginsdk

import (
	"context"
	"crypto/sha256"
	"testing"

	protocol "github.com/onelastleaf/go-plugin-sdk/protocol"
)

func TestIdentityAndStringResult(t *testing.T) {
	if _, err := New("invalid", "0.1.0").Build(); err == nil {
		t.Fatal("accepted invalid plugin ID")
	}
	if got := StringResult("value").String(); got != "value" {
		t.Fatalf("got %q", got)
	}
	if !canonicalUUIDV4("0f337c0c-51d6-44a9-a691-a31fce775ab1") ||
		canonicalUUIDV4("0f337c0c-51d6-14a9-a691-a31fce775ab1") {
		t.Fatal("canonical UUID v4 validation is incorrect")
	}
}

func TestEndpointMustBeLoopback(t *testing.T) {
	for _, invalid := range []string{"https://127.0.0.1:1", "http://example.com:1", "http://127.0.0.1"} {
		if _, err := validateEndpoint(invalid); err == nil {
			t.Fatalf("accepted %q", invalid)
		}
	}
}

func TestArtifactMetadataIsValidatedBeforeTransfer(t *testing.T) {
	bytes := []byte("artifact")
	digest := sha256.Sum256(bytes)
	host := &Host{maximumArtifactChunkSize: 1024}
	descriptor := &protocol.ArtifactDescriptor{
		ArtifactId: &protocol.PluginArtifactId{Value: "4b87847d-119a-4d53-aeb8-6da91cbff4e7"},
		FileName:   "artifact.txt",
		MediaType:  "text/plain",
		SizeBytes:  uint64(len(bytes)) + 1,
		Sha256:     digest[:],
	}
	if _, err := host.StoreArtifact(context.Background(), &protocol.TraceContext{CorrelationId: "test"}, "job", descriptor, [][]byte{bytes}); err == nil {
		t.Fatal("artifact with incorrect size was accepted")
	}
}

func TestEnvelopeDepthLimits(t *testing.T) {
	lastID := uint64(0)
	envelope := &protocol.PluginEnvelope{
		MessageId: 1,
		Trace:     &protocol.TraceContext{CorrelationId: "test", CallDepth: 2},
	}
	if err := validateEnvelope(envelope, &lastID, "", "", 1, 1); err == nil {
		t.Fatal("envelope above the negotiated call depth was accepted")
	}
}
