package main

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	sdkmath "cosmossdk.io/math"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	"github.com/cosmos/cosmos-sdk/crypto/keys/ed25519"
	sdk "github.com/cosmos/cosmos-sdk/types"
	querytypes "github.com/cosmos/cosmos-sdk/types/query"
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
	validators             []stakingtypes.Validator
	pool                   stakingtypes.Pool
	bondDenom              string
	delegations            []stakingtypes.DelegationResponse
	rejectOffsetPagination bool
	rejectAllPagination    bool
	delegationCalls        atomic.Int64
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
	if s.delegations == nil {
		return &stakingtypes.QueryValidatorDelegationsResponse{}, nil
	}

	s.delegationCalls.Add(1)

	page := req.Pagination
	// A chain whose delegation store is too large to scan at all refuses
	// every page size (Sei with a sparse validator).
	if s.rejectAllPagination {
		return nil, status.Error(codes.InvalidArgument,
			"scanned more than 10000 entries without filling the page; use a more specific key prefix or reduce limit")
	}
	// Sei rejects offset-mode queries that would scan too much of the store.
	if s.rejectOffsetPagination && len(page.GetKey()) == 0 {
		return nil, status.Error(codes.InvalidArgument,
			"scanned more than 10000 entries without filling the page; use key-based pagination instead")
	}

	start := 0
	if key := page.GetKey(); len(key) > 0 && !bytes.Equal(key, firstPageKey) {
		parsed, err := strconv.Atoi(string(key))
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "bad page key %q", key)
		}
		start = parsed
	}

	end := start + int(page.GetLimit())
	if end > len(s.delegations) {
		end = len(s.delegations)
	}

	response := &stakingtypes.QueryValidatorDelegationsResponse{
		DelegationResponses: s.delegations[start:end],
		Pagination:          &querytypes.PageResponse{},
	}
	if end < len(s.delegations) {
		response.Pagination.NextKey = []byte(strconv.Itoa(end))
	}
	return response, nil
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
	MaxDelegations = 0
	SkipValidatorDelegations = false
	delegationsUnsupported.Store(false)
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

func TestCollectValidatorDelegationsOnChainRejectingOffsetPagination(t *testing.T) {
	setTestGlobals(t)
	Limit = 100
	MaxDelegations = 0

	key := ed25519.GenPrivKey().PubKey().(*ed25519.PubKey)
	validator := makeTestValidator(t, "seivaloper1ccc", "noders", key)

	const delegatorCount = 250
	delegations := make([]stakingtypes.DelegationResponse, 0, delegatorCount)
	for i := 0; i < delegatorCount; i++ {
		delegations = append(delegations, stakingtypes.DelegationResponse{
			Delegation: stakingtypes.Delegation{
				DelegatorAddress: fmt.Sprintf("sei1delegator%d", i),
				ValidatorAddress: "seivaloper1ccc",
			},
			Balance: sdk.NewCoin("usei", sdkmath.NewInt(2_000_000)),
		})
	}

	staking := &fakeStakingServer{
		validators:             []stakingtypes.Validator{validator},
		delegations:            delegations,
		rejectOffsetPagination: true,
	}
	conn := startFakeNode(t, staking, &fakeSlashingServer{})

	registry, err := collectValidatorMetrics(context.Background(), conn, "seivaloper1ccc")
	if err != nil {
		t.Fatalf("collectValidatorMetrics failed: %v", err)
	}

	// Every delegator must be present despite the node refusing offset mode.
	for _, i := range []int{0, 149, delegatorCount - 1} {
		value, found := findGaugeValue(t, registry, "cosmos_validator_delegations", map[string]string{
			"delegated_by": fmt.Sprintf("sei1delegator%d", i),
		})
		if !found {
			t.Fatalf("delegation from sei1delegator%d missing — key-based pagination fallback did not collect all pages", i)
		}
		if value != 2 {
			t.Errorf("delegation value = %v, want 2 (2_000_000 / 1_000_000)", value)
		}
	}
}

func TestCollectValidatorStopsRetryingRefusedDelegations(t *testing.T) {
	setTestGlobals(t)

	key := ed25519.GenPrivKey().PubKey().(*ed25519.PubKey)
	validator := makeTestValidator(t, "seivaloper1eee", "noders", key)
	consAddr, err := consAddressFromValidator(validator, ConsensusNodePrefix)
	if err != nil {
		t.Fatalf("failed to derive consensus address: %v", err)
	}

	staking := &fakeStakingServer{
		validators:          []stakingtypes.Validator{validator},
		delegations:         []stakingtypes.DelegationResponse{{}},
		rejectAllPagination: true,
	}
	slashing := &fakeSlashingServer{
		signingInfos: []slashingtypes.ValidatorSigningInfo{{Address: consAddr, MissedBlocksCounter: 3}},
	}
	conn := startFakeNode(t, staking, slashing)

	if _, err := collectValidatorMetrics(context.Background(), conn, "seivaloper1eee"); err != nil {
		t.Fatalf("first collection failed: %v", err)
	}
	callsAfterFirst := staking.delegationCalls.Load()
	if callsAfterFirst == 0 {
		t.Fatal("expected the first collection to attempt the delegations query")
	}

	registry, err := collectValidatorMetrics(context.Background(), conn, "seivaloper1eee")
	if err != nil {
		t.Fatalf("second collection failed: %v", err)
	}

	if got := staking.delegationCalls.Load(); got != callsAfterFirst {
		t.Errorf("delegations query was retried (%d -> %d calls); a refused query must not hammer the node every refresh",
			callsAfterFirst, got)
	}

	// Everything else must keep working after the metric is latched off.
	if missed, found := findGaugeValue(t, registry, "cosmos_validator_missed_blocks", nil); !found || missed != 3 {
		t.Errorf("missed blocks = %v (found=%v), want 3", missed, found)
	}
}

func TestCollectValidatorSkipsDelegationsWhenDisabled(t *testing.T) {
	setTestGlobals(t)
	SkipValidatorDelegations = true
	t.Cleanup(func() { SkipValidatorDelegations = false })

	key := ed25519.GenPrivKey().PubKey().(*ed25519.PubKey)
	validator := makeTestValidator(t, "seivaloper1ddd", "noders", key)
	staking := &fakeStakingServer{
		validators: []stakingtypes.Validator{validator},
		delegations: []stakingtypes.DelegationResponse{{
			Delegation: stakingtypes.Delegation{DelegatorAddress: "sei1x", ValidatorAddress: "seivaloper1ddd"},
			Balance:    sdk.NewCoin("usei", sdkmath.NewInt(1)),
		}},
	}
	conn := startFakeNode(t, staking, &fakeSlashingServer{})

	registry, err := collectValidatorMetrics(context.Background(), conn, "seivaloper1ddd")
	if err != nil {
		t.Fatalf("collectValidatorMetrics failed: %v", err)
	}

	if _, found := findGaugeValue(t, registry, "cosmos_validator_delegations", nil); found {
		t.Error("per-delegator metrics present despite --skip-validator-delegations")
	}
	if _, found := findGaugeValue(t, registry, "cosmos_validator_tokens", nil); !found {
		t.Error("other validator metrics must still be collected")
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
