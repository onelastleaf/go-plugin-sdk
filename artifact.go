package pluginsdk

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"unicode/utf8"

	protocol "github.com/onelastleaf/go-plugin-sdk/protocol"
)

const artifactReadBufferBytes = 1 << 20

func validateArtifactInput(input ArtifactInput) error {
	if !canonicalUUIDV4(input.ID) {
		return errors.New("artifact ID must be a canonical UUID v4")
	}
	if input.FileName == "" || len(input.FileName) > 191 || !utf8.ValidString(input.FileName) {
		return errors.New("artifact file name must be valid UTF-8 between 1 and 191 bytes")
	}
	if input.FileName == "." || input.FileName == ".." || strings.ContainsAny(input.FileName, "/\\\x00") {
		return errors.New("artifact file name must be a safe base name")
	}
	if input.MediaType == "" {
		return errors.New("artifact media type must not be empty")
	}
	if input.Source == nil {
		return errors.New("artifact source must not be nil")
	}
	return nil
}

func (host *hostClient) storeArtifact(ctx context.Context, trace *protocol.TraceContext, jobID string, input ArtifactInput) (*protocol.ArtifactDescriptor, error) {
	if err := validateArtifactInput(input); err != nil {
		return nil, err
	}
	if !canonicalUUIDV4(jobID) {
		return nil, errors.New("artifact job ID must be a canonical UUID v4")
	}
	if host.maximumArtifactChunkSize == 0 {
		return nil, errors.New("host advertised a zero artifact chunk limit")
	}

	chunkSize := host.maximumArtifactChunkSize
	if chunkSize > artifactReadBufferBytes {
		chunkSize = artifactReadBufferBytes
	}
	buffer := make([]byte, int(chunkSize))
	// Hash first so oll receives authoritative metadata before any bytes. The
	// second pass verifies that a mutable source did not change mid-transfer.
	size, digest, err := hashArtifact(ctx, input.Source, buffer)
	if err != nil {
		return nil, fmt.Errorf("hash artifact: %w", err)
	}
	if size == 0 {
		return nil, errors.New("artifact must not be empty")
	}
	chunkCount := 1 + (size-1)/chunkSize
	if chunkCount > math.MaxUint32 {
		return nil, errors.New("artifact has too many chunks")
	}
	if err := seekArtifactStart(input.Source); err != nil {
		return nil, err
	}

	descriptor := &protocol.ArtifactDescriptor{
		ArtifactId: &protocol.PluginArtifactId{Value: input.ID},
		FileName:   input.FileName,
		MediaType:  input.MediaType,
		SizeBytes:  size,
		Sha256:     digest,
	}
	response, err := host.request(ctx, trace, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_ArtifactStart{ArtifactStart: &protocol.ArtifactTransferStart{
		JobId:      &protocol.PluginJobId{Value: jobID},
		Artifact:   descriptor,
		ChunkCount: uint32(chunkCount),
	}}})
	if err != nil {
		return nil, err
	}
	switch payload := response.Payload.(type) {
	case *protocol.PluginEnvelope_ArtifactAccepted:
		if payload.ArtifactAccepted.GetArtifactId().GetValue() != input.ID {
			return nil, ProtocolError{"host accepted another artifact ID"}
		}
	case *protocol.PluginEnvelope_ProtocolError:
		return nil, newHostError(payload.ProtocolError)
	default:
		return nil, ProtocolError{"host did not accept the artifact transfer"}
	}

	secondHash := sha256.New()
	remaining := size
	for index := uint32(0); index < uint32(chunkCount); index++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		wanted := chunkSize
		if remaining < wanted {
			wanted = remaining
		}
		chunk := buffer[:int(wanted)]
		if err := readArtifactFull(ctx, input.Source, chunk); err != nil {
			return nil, fmt.Errorf("read artifact chunk %d: %w", index, err)
		}
		_, _ = secondHash.Write(chunk)
		if _, err := host.sender.send(nil, trace, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_ArtifactChunk{ArtifactChunk: &protocol.ArtifactTransferChunk{
			ArtifactId: descriptor.ArtifactId,
			ChunkIndex: index,
			Data:       chunk,
		}}}); err != nil {
			return nil, fmt.Errorf("send artifact chunk %d: %w", index, err)
		}
		remaining -= wanted
	}
	if remaining != 0 {
		return nil, errors.New("artifact source ended before its declared size")
	}
	extra := []byte{0}
	if err := readArtifactFull(ctx, input.Source, extra); err != io.EOF {
		if err == nil {
			return nil, errors.New("artifact source grew while it was being transferred")
		}
		return nil, fmt.Errorf("check artifact end: %w", err)
	}
	if !bytes.Equal(secondHash.Sum(nil), digest) {
		return nil, errors.New("artifact source changed while it was being transferred")
	}

	response, err = host.request(ctx, trace, &protocol.PluginEnvelope{Payload: &protocol.PluginEnvelope_ArtifactComplete{ArtifactComplete: &protocol.ArtifactTransferComplete{ArtifactId: descriptor.ArtifactId}}})
	if err != nil {
		return nil, err
	}
	switch payload := response.Payload.(type) {
	case *protocol.PluginEnvelope_ArtifactStored:
		if payload.ArtifactStored.GetArtifactId().GetValue() != input.ID {
			return nil, ProtocolError{"host stored another artifact ID"}
		}
	case *protocol.PluginEnvelope_ProtocolError:
		return nil, newHostError(payload.ProtocolError)
	default:
		return nil, ProtocolError{"host did not acknowledge the stored artifact"}
	}
	return cloneArtifactDescriptor(descriptor), nil
}

func readArtifactFull(ctx context.Context, source io.Reader, buffer []byte) error {
	offset := 0
	for offset < len(buffer) {
		if err := ctx.Err(); err != nil {
			return err
		}
		count, err := source.Read(buffer[offset:])
		offset += count
		if offset == len(buffer) {
			return nil
		}
		if err == io.EOF {
			if offset == 0 {
				return io.EOF
			}
			return io.ErrUnexpectedEOF
		}
		if err != nil {
			return err
		}
		if count == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}

func hashArtifact(ctx context.Context, source io.ReadSeeker, buffer []byte) (uint64, []byte, error) {
	if err := seekArtifactStart(source); err != nil {
		return 0, nil, err
	}
	hash := sha256.New()
	var size uint64
	for {
		if err := ctx.Err(); err != nil {
			return 0, nil, err
		}
		count, readErr := source.Read(buffer)
		if count > 0 {
			if uint64(count) > math.MaxUint64-size {
				return 0, nil, errors.New("artifact size overflowed")
			}
			size += uint64(count)
			_, _ = hash.Write(buffer[:count])
		}
		if readErr == io.EOF {
			return size, hash.Sum(nil), nil
		}
		if readErr != nil {
			return 0, nil, readErr
		}
		if count == 0 {
			return 0, nil, io.ErrNoProgress
		}
	}
}

func seekArtifactStart(source io.Seeker) error {
	offset, err := source.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("seek artifact source: %w", err)
	}
	if offset != 0 {
		return errors.New("artifact source did not seek to its beginning")
	}
	return nil
}
