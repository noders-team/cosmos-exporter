package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/cosmos/cosmos-sdk/crypto/keys/ed25519"
	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
	"github.com/cosmos/cosmos-sdk/types/bech32"
	stakingtypes "github.com/cosmos/cosmos-sdk/x/staking/types"
)

const (
	ed25519PubKeyTypeURL   = "/cosmos.crypto.ed25519.PubKey"
	secp256k1PubKeyTypeURL = "/cosmos.crypto.secp256k1.PubKey"
)

// consAddressFromValidator derives the bech32-encoded consensus address of a
// validator from its consensus pubkey. Standard key types are handled via the
// SDK; unknown types (e.g. Union's bn254) fall back to CometBFT's generic
// address rule: sha256(raw key bytes)[:20].
func consAddressFromValidator(validator stakingtypes.Validator, prefix string) (string, error) {
	anyPk := validator.ConsensusPubkey
	if anyPk == nil {
		return "", fmt.Errorf("validator %s has no consensus pubkey", validator.OperatorAddress)
	}

	var addrBytes []byte
	switch anyPk.TypeUrl {
	case ed25519PubKeyTypeURL:
		var pk ed25519.PubKey
		if err := pk.Unmarshal(anyPk.Value); err != nil {
			return "", fmt.Errorf("failed to unmarshal ed25519 pubkey: %w", err)
		}
		// pk.Address() паникует на ключе неверной длины, а байты приходят
		// из внешнего ответа ноды — валидируем сами.
		if len(pk.Key) != ed25519.PubKeySize {
			return "", fmt.Errorf("ed25519 pubkey has wrong size: %d bytes, want %d", len(pk.Key), ed25519.PubKeySize)
		}
		addrBytes = pk.Address()
	case secp256k1PubKeyTypeURL:
		var pk secp256k1.PubKey
		if err := pk.Unmarshal(anyPk.Value); err != nil {
			return "", fmt.Errorf("failed to unmarshal secp256k1 pubkey: %w", err)
		}
		if len(pk.Key) != secp256k1.PubKeySize {
			return "", fmt.Errorf("secp256k1 pubkey has wrong size: %d bytes, want %d", len(pk.Key), secp256k1.PubKeySize)
		}
		addrBytes = pk.Address()
	default:
		key, err := rawPubkeyFromAnyValue(anyPk.Value)
		if err != nil {
			return "", fmt.Errorf("failed to extract raw pubkey from %s: %w", anyPk.TypeUrl, err)
		}
		sum := sha256.Sum256(key)
		addrBytes = sum[:20]
	}

	encoded, err := bech32.ConvertAndEncode(prefix, addrBytes)
	if err != nil {
		return "", fmt.Errorf("failed to bech32-encode consensus address: %w", err)
	}
	return encoded, nil
}

// rawPubkeyFromAnyValue extracts the raw key bytes from a serialized pubkey
// proto message. All known Cosmos pubkey messages store the key as a single
// length-delimited field 1 (wire tag 0x0a).
func rawPubkeyFromAnyValue(value []byte) ([]byte, error) {
	if len(value) < 2 {
		return nil, fmt.Errorf("pubkey proto value too short: %d bytes", len(value))
	}
	if value[0] != 0x0a {
		return nil, fmt.Errorf("unexpected proto wire tag 0x%02x, want 0x0a", value[0])
	}

	keyLen, n := binary.Uvarint(value[1:])
	if n <= 0 {
		return nil, fmt.Errorf("could not decode pubkey length varint")
	}

	start := 1 + n
	end := start + int(keyLen)
	if keyLen == 0 || end > len(value) {
		return nil, fmt.Errorf("declared pubkey length %d exceeds value size %d", keyLen, len(value))
	}
	return value[start:end], nil
}
