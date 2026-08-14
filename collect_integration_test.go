package main

import (
	"context"
	"math/big"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	sdkmath "cosmossdk.io/math"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	"github.com/cosmos/cosmos-sdk/crypto/keys/ed25519"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	distributiontypes "github.com/cosmos/cosmos-sdk/x/distribution/types"
	minttypes "github.com/cosmos/cosmos-sdk/x/mint/types"
	slashingtypes "github.com/cosmos/cosmos-sdk/x/slashing/types"
	stakingtypes "github.com/cosmos/cosmos-sdk/x/staking/types"
	"github.com/prometheus/client_golang/prometheus"
)

// --- fake gRPC servers ---

type fakeStakingServer struct {
	stakingtypes.UnimplementedQueryServer
	validators []stakingtypes.Validator
	pool       stakingtypes.Pool
	bondDenom  string
}

func (s *fakeStakingServer) Validator(ctx context.Context, req *stakingtypes.QueryValidatorRequest) (*stakingtypes.QueryValidatorResponse, error) {
	for _, v := range s.validators {
		if v.OperatorAddress == req.ValidatorAddr {
			return &stakingtypes.QueryValidatorResponse{Validator: v}, nil
		}
	}
	return nil, status.Error(codes.NotFound, "validator not found")
}

func (s *fakeStakingServer) Validators(ctx context.Context, req *stakingtypes.QueryValidatorsRequest) (*stakingtypes.QueryValidatorsResponse, error) {
	return &stakingtypes.QueryValidatorsResponse{Validators: s.validators}, nil
}

func (s *fakeStakingServer) Params(ctx context.Context, req *stakingtypes.QueryParamsRequest) (*stakingtypes.QueryParamsResponse, error) {
	return &stakingtypes.QueryParamsResponse{
		Params: stakingtypes.Params{MaxValidators: 100, BondDenom: s.bondDenom},
	}, nil
}

func (s *fakeStakingServer) Pool(ctx context.Context, req *stakingtypes.QueryPoolRequest) (*stakingtypes.QueryPoolResponse, error) {
	return &stakingtypes.QueryPoolResponse{Pool: s.pool}, nil
}

func (s *fakeStakingServer) ValidatorDelegations(ctx context.Context, req *stakingtypes.QueryValidatorDelegationsRequest) (*stakingtypes.QueryValidatorDelegationsResponse, error) {
	return &stakingtypes.QueryValidatorDelegationsResponse{}, nil
}

func (s *fakeStakingServer) ValidatorUnbondingDelegations(ctx context.Context, req *stakingtypes.QueryValidatorUnbondingDelegationsRequest) (*stakingtypes.QueryValidatorUnbondingDelegationsResponse, error) {
	return &stakingtypes.QueryValidatorUnbondingDelegationsResponse{}, nil
}

func (s *fakeStakingServer) Redelegations(ctx context.Context, req *stakingtypes.QueryRedelegationsRequest) (*stakingtypes.QueryRedelegationsResponse, error) {
	return &stakingtypes.QueryRedelegationsResponse{}, nil
}

type fakeSlashingServer struct {
	slashingtypes.UnimplementedQueryServer
	mu             sync.Mutex
	signingInfos   []slashingtypes.ValidatorSigningInfo
	requestedAddrs []string
}

func (s *fakeSlashingServer) SigningInfo(ctx context.Context, req *slashingtypes.QuerySigningInfoRequest) (*slashingtypes.QuerySigningInfoResponse, error) {
	s.mu.Lock()
	s.requestedAddrs = append(s.requestedAddrs, req.ConsAddress)
	s.mu.Unlock()

	for _, info := range s.signingInfos {
		if info.Address == req.ConsAddress {
			return &slashingtypes.QuerySigningInfoResponse{ValSigningInfo: info}, nil
		}
	}
	return nil, status.Error(codes.NotFound, "signing info not found")
}

func (s *fakeSlashingServer) SigningInfos(ctx context.Context, req *slashingtypes.QuerySigningInfosRequest) (*slashingtypes.QuerySigningInfosResponse, error) {
	return &slashingtypes.QuerySigningInfosResponse{Info: s.signingInfos}, nil
}

// failingMintServer имитирует кастомный mint-модуль Sei: стандартные запросы падают.
type failingMintServer struct {
	minttypes.UnimplementedQueryServer
}

type fakeDistributionServer struct {
	distributiontypes.UnimplementedQueryServer
}

func (s *fakeDistributionServer) CommunityPool(ctx context.Context, req *distributiontypes.QueryCommunityPoolRequest) (*distributiontypes.QueryCommunityPoolResponse, error) {
	return &distributiontypes.QueryCommunityPoolResponse{}, nil
}

func (s *fakeDistributionServer) ValidatorCommission(ctx context.Context, req *distributiontypes.QueryValidatorCommissionRequest) (*distributiontypes.QueryValidatorCommissionResponse, error) {
	return &distributiontypes.QueryValidatorCommissionResponse{}, nil
}

func (s *fakeDistributionServer) ValidatorOutstandingRewards(ctx context.Context, req *distributiontypes.QueryValidatorOutstandingRewardsRequest) (*distributiontypes.QueryValidatorOutstandingRewardsResponse, error) {
	return &distributiontypes.QueryValidatorOutstandingRewardsResponse{}, nil
}

// --- test harness ---

func startFakeNode(t *testing.T, staking *fakeStakingServer, slashing *fakeSlashingServer) *grpc.ClientConn {
	t.Helper()

	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	stakingtypes.RegisterQueryServer(server, staking)
	slashingtypes.RegisterQueryServer(server, slashing)
	minttypes.RegisterQueryServer(server, &failingMintServer{})
	distributiontypes.RegisterQueryServer(server, &fakeDistributionServer{})
	banktypes.RegisterQueryServer(server, &banktypes.UnimplementedQueryServer{})

	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial bufconn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

func setTestGlobals(t *testing.T) {
	t.Helper()
	Denom = "usei"
	DenomCoefficient = 1_000_000
	ConsensusNodePrefix = "seivalcons"
	ConstLabels = prometheus.Labels{"chain_id": "sei-test-1"}
	Limit = 1000
}

func makeTestValidator(t *testing.T, operator, moniker string, missedBlocksKey *ed25519.PubKey) stakingtypes.Validator {
	t.Helper()
	anyPk, err := codectypes.NewAnyWithValue(missedBlocksKey)
	if err != nil {
		t.Fatalf("failed to pack pubkey: %v", err)
	}
	return stakingtypes.Validator{
		OperatorAddress:   operator,
		ConsensusPubkey:   anyPk,
		Status:            stakingtypes.Bonded,
		Tokens:            sdkmath.NewInt(5_000_000),
		DelegatorShares:   sdkmath.LegacyNewDec(5_000_000),
		MinSelfDelegation: sdkmath.NewInt(1),
		Description:       stakingtypes.Description{Moniker: moniker},
		Commission: stakingtypes.Commission{
			CommissionRates: stakingtypes.CommissionRates{
				Rate:          sdkmath.LegacyNewDecWithPrec(5, 2),
				MaxRate:       sdkmath.LegacyNewDecWithPrec(20, 2),
				MaxChangeRate: sdkmath.LegacyNewDecWithPrec(1, 2),
			},
		},
	}
}

func findGaugeValue(t *testing.T, registry *prometheus.Registry, name string, wantLabels map[string]string) (float64, bool) {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, pair := range metric.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
			matched := true
			for k, v := range wantLabels {
				if labels[k] != v {
					matched = false
					break
				}
			}
			if matched {
				return metric.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

// --- tests ---

func TestCollectValidatorsMissedBlocksOnSeiLikeChain(t *testing.T) {
	setTestGlobals(t)

	key := ed25519.GenPrivKey().PubKey().(*ed25519.PubKey)
	validator := makeTestValidator(t, "seivaloper1aaa", "noders", key)

	consAddr, err := consAddressFromValidator(validator, ConsensusNodePrefix)
	if err != nil {
		t.Fatalf("failed to derive consensus address: %v", err)
	}

	staking := &fakeStakingServer{validators: []stakingtypes.Validator{validator}}
	slashing := &fakeSlashingServer{
		signingInfos: []slashingtypes.ValidatorSigningInfo{
			{Address: consAddr, MissedBlocksCounter: 42},
		},
	}
	conn := startFakeNode(t, staking, slashing)

	registry, err := collectValidatorsMetrics(context.Background(), conn)
	if err != nil {
		t.Fatalf("collectValidatorsMetrics failed: %v", err)
	}

	missed, found := findGaugeValue(t, registry, "cosmos_validators_missed_blocks", map[string]string{
		"address": "seivaloper1aaa",
		"moniker": "noders",
	})
	if !found {
		t.Fatal("cosmos_validators_missed_blocks metric not found — consensus address matching is broken")
	}
	if missed != 42 {
		t.Errorf("missed blocks = %v, want 42", missed)
	}

	shares, found := findGaugeValue(t, registry, "cosmos_validators_delegator_shares", map[string]string{
		"address": "seivaloper1aaa",
	})
	if !found {
		t.Fatal("cosmos_validators_delegator_shares metric not found")
	}
	if shares != 5 {
		t.Errorf("delegator shares = %v, want 5 (5_000_000 / 1_000_000): LegacyDec scaling is broken", shares)
	}
}

func TestCollectValidatorMissedBlocksUsesDerivedBech32ConsAddress(t *testing.T) {
	setTestGlobals(t)

	key := ed25519.GenPrivKey().PubKey().(*ed25519.PubKey)
	validator := makeTestValidator(t, "seivaloper1bbb", "noders-solo", key)

	consAddr, err := consAddressFromValidator(validator, ConsensusNodePrefix)
	if err != nil {
		t.Fatalf("failed to derive consensus address: %v", err)
	}

	staking := &fakeStakingServer{validators: []stakingtypes.Validator{validator}}
	slashing := &fakeSlashingServer{
		signingInfos: []slashingtypes.ValidatorSigningInfo{
			{Address: consAddr, MissedBlocksCounter: 7},
		},
	}
	conn := startFakeNode(t, staking, slashing)

	registry, err := collectValidatorMetrics(context.Background(), conn, "seivaloper1bbb")
	if err != nil {
		t.Fatalf("collectValidatorMetrics failed: %v", err)
	}

	missed, found := findGaugeValue(t, registry, "cosmos_validator_missed_blocks", map[string]string{
		"address": "seivaloper1bbb",
	})
	if !found {
		t.Fatal("cosmos_validator_missed_blocks metric not found")
	}
	if missed != 7 {
		t.Errorf("missed blocks = %v, want 7", missed)
	}

	slashing.mu.Lock()
	defer slashing.mu.Unlock()
	if len(slashing.requestedAddrs) == 0 {
		t.Fatal("SigningInfo was never called")
	}
	for _, addr := range slashing.requestedAddrs {
		if addr != consAddr {
			t.Errorf("SigningInfo called with %q, want derived bech32 %q", addr, consAddr)
		}
	}
}

func TestCollectGeneralSurvivesHugeSupplyAndMissingMint(t *testing.T) {
	setTestGlobals(t)

	// 2^70 — заведомо больше int64: старый код паниковал на Int64().
	hugeAmount := sdkmath.NewIntFromBigInt(new(big.Int).Lsh(big.NewInt(1), 70))

	staking := &fakeStakingServer{
		pool: stakingtypes.Pool{
			BondedTokens:    hugeAmount,
			NotBondedTokens: sdkmath.NewInt(1),
		},
	}
	conn := startFakeNode(t, staking, &fakeSlashingServer{})

	registry, err := collectGeneralMetrics(context.Background(), conn)
	if err != nil {
		t.Fatalf("collectGeneralMetrics failed: %v", err)
	}

	bonded, found := findGaugeValue(t, registry, "cosmos_general_bonded_tokens", nil)
	if !found {
		t.Fatal("cosmos_general_bonded_tokens metric not found")
	}
	expected, _ := new(big.Float).SetInt(hugeAmount.BigInt()).Float64()
	if bonded != expected {
		t.Errorf("bonded tokens = %v, want %v", bonded, expected)
	}

	// Кастомный mint-модуль недоступен: эндпоинт не должен падать целиком.
	// Плоский Gauge остаётся в выдаче со значением 0 (историческое поведение
	// экспортера, на него завязана совместимость).
	inflation, found := findGaugeValue(t, registry, "cosmos_general_inflation", nil)
	if !found {
		t.Error("cosmos_general_inflation family missing entirely")
	}
	if inflation != 0 {
		t.Errorf("inflation = %v, want 0 when mint module is unavailable", inflation)
	}
}
