package main

import (
	"crypto/sha256"
	"testing"

	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	"github.com/cosmos/cosmos-sdk/crypto/keys/ed25519"
	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
	cryptotypes "github.com/cosmos/cosmos-sdk/crypto/types"
	"github.com/cosmos/cosmos-sdk/types/bech32"
	stakingtypes "github.com/cosmos/cosmos-sdk/x/staking/types"
)

func mustAny(t *testing.T, pk cryptotypes.PubKey) *codectypes.Any {
	t.Helper()
	anyPk, err := codectypes.NewAnyWithValue(pk)
	if err != nil {
		t.Fatalf("failed to pack pubkey into Any: %v", err)
	}
	return anyPk
}

func TestConsAddressFromValidator(t *testing.T) {
	edKey := ed25519.GenPrivKey().PubKey()
	secpKey := secp256k1.GenPrivKey().PubKey()

	// Synthetic bn254-style pubkey: 32 raw bytes wrapped in a proto message
	// with a single length-delimited field 1, under an unknown type URL.
	bn254Raw := make([]byte, 32)
	for i := range bn254Raw {
		bn254Raw[i] = byte(i + 1)
	}
	bn254Value := append([]byte{0x0a, 0x20}, bn254Raw...)
	bn254Sum := sha256.Sum256(bn254Raw)

	expectBech32 := func(t *testing.T, prefix string, addr []byte) string {
		t.Helper()
		encoded, err := bech32.ConvertAndEncode(prefix, addr)
		if err != nil {
			t.Fatalf("failed to encode expected address: %v", err)
		}
		return encoded
	}

	tests := []struct {
		name    string
		pubkey  *codectypes.Any
		prefix  string
		want    string
		wantErr bool
	}{
		{
			name:   "ed25519 (Sei, Cosmos Hub)",
			pubkey: mustAny(t, edKey),
			prefix: "seivalcons",
			want:   expectBech32(t, "seivalcons", edKey.Address()),
		},
		{
			name:   "secp256k1",
			pubkey: mustAny(t, secpKey),
			prefix: "injvalcons",
			want:   expectBech32(t, "injvalcons", secpKey.Address()),
		},
		{
			name: "unknown key type falls back to sha256 of raw key (Union bn254)",
			pubkey: &codectypes.Any{
				TypeUrl: "/cometbft.crypto.v1.bn254.PubKey",
				Value:   bn254Value,
			},
			prefix: "unionvalcons",
			want:   expectBech32(t, "unionvalcons", bn254Sum[:20]),
		},
		{
			name:    "nil pubkey",
			pubkey:  nil,
			prefix:  "cosmosvalcons",
			wantErr: true,
		},
		{
			name: "garbage proto value",
			pubkey: &codectypes.Any{
				TypeUrl: "/some.unknown.PubKey",
				Value:   []byte{0xff},
			},
			prefix:  "cosmosvalcons",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := stakingtypes.Validator{
				OperatorAddress: "valoper-test",
				ConsensusPubkey: tt.pubkey,
			}
			got, err := consAddressFromValidator(validator, tt.prefix)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got address %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("consAddressFromValidator() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRawPubkeyFromAnyValue(t *testing.T) {
	tests := []struct {
		name    string
		value   []byte
		wantLen int
		wantErr bool
	}{
		{
			name:    "32-byte key",
			value:   append([]byte{0x0a, 0x20}, make([]byte, 32)...),
			wantLen: 32,
		},
		{
			name:    "empty value",
			value:   nil,
			wantErr: true,
		},
		{
			name:    "wrong field tag",
			value:   []byte{0x12, 0x02, 0x01, 0x02},
			wantErr: true,
		},
		{
			name:    "declared length exceeds data",
			value:   []byte{0x0a, 0x20, 0x01},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := rawPubkeyFromAnyValue(tt.value)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %d bytes", len(got))
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != tt.wantLen {
				t.Errorf("key length = %d, want %d", len(got), tt.wantLen)
			}
		})
	}
}
