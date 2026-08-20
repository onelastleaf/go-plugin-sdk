package pluginsdk

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"testing"
	"time"

	protocol "github.com/onelastleaf/go-plugin-sdk/protocol"
	"google.golang.org/protobuf/proto"
)

func TestStoreArtifactStreamsAndReturnsVerifiedDescriptor(t *testing.T) {
	stream := newFakePluginStream()
	host := newHostClient(newEnvelopeSender(stream), artifactReadBufferBytes, 8, 8)
	jobTrace := &protocol.TraceContext{CorrelationId: "artifact", ParentCallId: ref(uint64(4)), CallDepth: 2, CausalDepth: 3}
	action := newActionContext(context.Background(), testJobID(1), jobTrace, 9, host)
	data := bytes.Repeat([]byte("a"), 2*artifactReadBufferBytes+17)
	done := make(chan struct {
		descriptor *protocol.ArtifactDescriptor
		err        error
	}, 1)
	go func() {
		descriptor, err := action.StoreArtifact(ArtifactInput{
			ID:        testJobID(2),
			FileName:  "large.bin",
			MediaType: "application/octet-stream",
			Source:    bytes.NewReader(data),
		})
		done <- struct {
			descriptor *protocol.ArtifactDescriptor
			err        error
		}{descriptor, err}
	}()

	startEnvelope := nextSent(t, stream)
	if !proto.Equal(startEnvelope.Trace, jobTrace) {
		t.Fatalf("artifact start trace = %v; want %v", startEnvelope.Trace, jobTrace)
	}
	start := startEnvelope.GetArtifactStart()
	if start == nil || start.ChunkCount != 3 || start.Artifact.SizeBytes != uint64(len(data)) {
		t.Fatalf("artifact start = %v", start)
	}
	if err := host.route(&protocol.PluginEnvelope{
		ReplyTo: ref(startEnvelope.MessageId),
		Trace:   cloneTrace(startEnvelope.Trace),
		Payload: &protocol.PluginEnvelope_ArtifactAccepted{ArtifactAccepted: &protocol.ArtifactTransferAccepted{ArtifactId: start.Artifact.ArtifactId}},
	}); err != nil {
		t.Fatal(err)
	}

	var transferred []byte
	for index := uint32(0); index < start.ChunkCount; index++ {
		envelope := nextSent(t, stream)
		if !proto.Equal(envelope.Trace, jobTrace) {
			t.Fatalf("artifact chunk trace = %v; want %v", envelope.Trace, jobTrace)
		}
		chunk := envelope.GetArtifactChunk()
		if chunk == nil || chunk.ChunkIndex != index || len(chunk.Data) > artifactReadBufferBytes {
			t.Fatalf("artifact chunk %d = %v", index, chunk)
		}
		transferred = append(transferred, chunk.Data...)
	}
	if !bytes.Equal(transferred, data) {
		t.Fatal("streamed artifact differs from its source")
	}
	completeEnvelope := nextSent(t, stream)
	if !proto.Equal(completeEnvelope.Trace, jobTrace) {
		t.Fatalf("artifact completion trace = %v; want %v", completeEnvelope.Trace, jobTrace)
	}
	complete := completeEnvelope.GetArtifactComplete()
	if complete == nil {
		t.Fatalf("expected ArtifactTransferComplete, got %T", completeEnvelope.Payload)
	}
	if err := host.route(&protocol.PluginEnvelope{
		ReplyTo: ref(completeEnvelope.MessageId),
		Trace:   cloneTrace(completeEnvelope.Trace),
		Payload: &protocol.PluginEnvelope_ArtifactStored{ArtifactStored: &protocol.ArtifactStored{ArtifactId: complete.ArtifactId}},
	}); err != nil {
		t.Fatal(err)
	}
	result := <-done
	if result.err != nil {
		t.Fatal(result.err)
	}
	digest := sha256.Sum256(data)
	if result.descriptor.SizeBytes != uint64(len(data)) || !bytes.Equal(result.descriptor.Sha256, digest[:]) {
		t.Fatalf("descriptor = %v", result.descriptor)
	}
	if _, err := action.cloneAndValidateResult(ActionResult{Artifacts: []*protocol.ArtifactDescriptor{result.descriptor}}); err != nil {
		t.Fatalf("stored descriptor was rejected from the action result: %v", err)
	}
	changed := cloneArtifactDescriptor(result.descriptor)
	changed.SizeBytes++
	if _, err := action.cloneAndValidateResult(ActionResult{Artifacts: []*protocol.ArtifactDescriptor{changed}}); err == nil {
		t.Fatal("action result accepted a descriptor that oll did not store")
	}
}

func TestArtifactInputRejectsUnsafeNames(t *testing.T) {
	for _, name := range []string{"", ".", "..", "../escape", `dir\\file`, "nul\x00file"} {
		if err := validateArtifactInput(ArtifactInput{ID: testJobID(1), FileName: name, MediaType: "text/plain", Source: bytes.NewReader([]byte("x"))}); err == nil {
			t.Errorf("accepted unsafe artifact name %q", name)
		}
	}
}

func TestArtifactSeekAndReadErrorsAreReturnedBeforeTransfer(t *testing.T) {
	stream := newFakePluginStream()
	host := newHostClient(newEnvelopeSender(stream), 1024, 8, 8)
	trace := &protocol.TraceContext{CorrelationId: "artifact"}
	base := ArtifactInput{ID: testJobID(1), FileName: "file.bin", MediaType: "application/octet-stream"}

	seekFailure := errors.New("seek failed")
	base.Source = failingArtifactSource{seekErr: seekFailure}
	if _, err := host.storeArtifact(context.Background(), trace, testJobID(2), base); !errors.Is(err, seekFailure) {
		t.Fatalf("seek error = %v; want %v", err, seekFailure)
	}
	readFailure := errors.New("read failed")
	base.Source = &failingArtifactSource{readErr: readFailure}
	if _, err := host.storeArtifact(context.Background(), trace, testJobID(2), base); !errors.Is(err, readFailure) {
		t.Fatalf("read error = %v; want %v", err, readFailure)
	}
	assertNoSent(t, stream, 20*time.Millisecond)
}

func TestArtifactReaderThatMakesNoProgressIsRejected(t *testing.T) {
	err := readArtifactFull(context.Background(), zeroProgressReader{}, make([]byte, 1))
	if !errors.Is(err, io.ErrNoProgress) {
		t.Fatalf("read error = %v; want io.ErrNoProgress", err)
	}
}

func TestArtifactProtocolErrorPreservesStructuredDetail(t *testing.T) {
	stream := newFakePluginStream()
	host := newHostClient(newEnvelopeSender(stream), 1024, 8, 8)
	action := newActionContext(context.Background(), testJobID(1), &protocol.TraceContext{CorrelationId: "artifact"}, 9, host)
	done := make(chan error, 1)
	go func() {
		_, err := action.StoreArtifact(ArtifactInput{
			ID:        testJobID(2),
			FileName:  "artifact.txt",
			MediaType: "text/plain",
			Source:    bytes.NewReader([]byte("payload")),
		})
		done <- err
	}()
	request := nextSent(t, stream)
	detail := &protocol.ProtocolError{
		Code:      protocol.ErrorCode_ERROR_CODE_FAILED_PRECONDITION,
		Message:   "artifact rejected",
		Retryable: true,
		Metadata:  map[string]string{"reason": "quota"},
	}
	if err := host.route(&protocol.PluginEnvelope{
		ReplyTo: ref(request.MessageId),
		Trace:   cloneTrace(request.Trace),
		Payload: &protocol.PluginEnvelope_ProtocolError{ProtocolError: detail},
	}); err != nil {
		t.Fatal(err)
	}
	err := <-done
	var hostErr *HostError
	if !errors.As(err, &hostErr) || hostErr.Code() != detail.Code || hostErr.Detail().Metadata["reason"] != "quota" {
		t.Fatalf("artifact error = %v; want complete HostError %v", err, detail)
	}
}

type failingArtifactSource struct {
	seekErr error
	readErr error
}

func (source failingArtifactSource) Read([]byte) (int, error) {
	if source.readErr != nil {
		return 0, source.readErr
	}
	return 0, io.EOF
}

func (source failingArtifactSource) Seek(int64, int) (int64, error) {
	if source.seekErr != nil {
		return 0, source.seekErr
	}
	return 0, nil
}

type zeroProgressReader struct{}

func (zeroProgressReader) Read([]byte) (int, error) { return 0, nil }
