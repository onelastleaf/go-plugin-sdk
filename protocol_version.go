package pluginsdk

import "encoding/hex"

// ProtocolSchemaSHA256 is the published oll protocol fingerprint supported by
// this SDK release.
const ProtocolSchemaSHA256 = "21fd97cf8ec1a89ef464192fe69d123469410c40910d3a7b74898224da61545a"

var protocolSchemaFingerprint = mustDecodeProtocolFingerprint(ProtocolSchemaSHA256)

func mustDecodeProtocolFingerprint(value string) []byte {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 32 {
		panic("pluginsdk: invalid compiled protocol fingerprint")
	}
	return decoded
}
