package main

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc"

	querytypes "github.com/cosmos/cosmos-sdk/types/query"
	slashingtypes "github.com/cosmos/cosmos-sdk/x/slashing/types"
	stakingtypes "github.com/cosmos/cosmos-sdk/x/staking/types"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

func collectValidatorsMetrics(ctx context.Context, grpcConn *grpc.ClientConn) (*prometheus.Registry, error) {
	requestStart := time.Now()

	sublogger := log.With().
		Str("request-id", uuid.New().String()).
		Logger()

	validatorsCommissionGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "cosmos_validators_commission",
			Help:        "Commission of the Cosmos-based blockchain validator",
			ConstLabels: ConstLabels,
		},
		[]string{"address", "moniker"},
	)

	validatorsStatusGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "cosmos_validators_status",
			Help:        "Status of the Cosmos-based blockchain validator",
			ConstLabels: ConstLabels,
		},
		[]string{"address", "moniker"},
	)

	validatorsJailedGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "cosmos_validators_jailed",
			Help:        "Jailed status of the Cosmos-based blockchain validator",
			ConstLabels: ConstLabels,
		},
		[]string{"address", "moniker"},
	)

	validatorsTokensGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "cosmos_validators_tokens",
			Help:        "Tokens of the Cosmos-based blockchain validator",
			ConstLabels: ConstLabels,
		},
		[]string{"address", "moniker", "denom"},
	)

	validatorsDelegatorSharesGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "cosmos_validators_delegator_shares",
			Help:        "Delegator shares of the Cosmos-based blockchain validator",
			ConstLabels: ConstLabels,
		},
		[]string{"address", "moniker", "denom"},
	)

	validatorsMinSelfDelegationGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "cosmos_validators_min_self_delegation",
			Help:        "Self-declared minimum self-delegation shares of the Cosmos-based blockchain validator",
			ConstLabels: ConstLabels,
		},
		[]string{"address", "moniker", "denom"},
	)

	validatorsMissedBlocksGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "cosmos_validators_missed_blocks",
			Help:        "Missed blocks of the Cosmos-based blockchain validator",
			ConstLabels: ConstLabels,
		},
		[]string{"address", "moniker"},
	)

	validatorsRankGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "cosmos_validators_rank",
			Help:        "Rank of the Cosmos-based blockchain validator",
			ConstLabels: ConstLabels,
		},
		[]string{"address", "moniker"},
	)

	validatorsIsActiveGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "cosmos_validators_active",
			Help:        "1 if the Cosmos-based blockchain validator is in active set, 0 if not",
			ConstLabels: ConstLabels,
		},
		[]string{"address", "moniker"},
	)

	registry := prometheus.NewRegistry()
	registry.MustRegister(validatorsCommissionGauge)
	registry.MustRegister(validatorsStatusGauge)
	registry.MustRegister(validatorsJailedGauge)
	registry.MustRegister(validatorsTokensGauge)
	registry.MustRegister(validatorsDelegatorSharesGauge)
	registry.MustRegister(validatorsMinSelfDelegationGauge)
	registry.MustRegister(validatorsMissedBlocksGauge)
	registry.MustRegister(validatorsRankGauge)
	registry.MustRegister(validatorsIsActiveGauge)

	var validators []stakingtypes.Validator
	var validatorsErr error
	var signingInfos []slashingtypes.ValidatorSigningInfo
	var validatorSetLength uint32

	var wg sync.WaitGroup

	goSafe(&wg, func() {
		sublogger.Debug().Msg("Started querying validators")
		queryStart := time.Now()

		stakingClient := stakingtypes.NewQueryClient(grpcConn)
		validatorsResponse, err := stakingClient.Validators(
			ctx,
			&stakingtypes.QueryValidatorsRequest{
				Pagination: &querytypes.PageRequest{
					Limit: Limit,
				},
			},
		)
		if err != nil {
			validatorsErr = fmt.Errorf("could not get validators: %w", err)
			return
		}

		sublogger.Debug().
			Float64("request-time", time.Since(queryStart).Seconds()).
			Msg("Finished querying validators")
		validators = validatorsResponse.Validators

		sort.Slice(validators, func(i, j int) bool {
			return validators[i].DelegatorShares.GT(validators[j].DelegatorShares)
		})
	})

	goSafe(&wg, func() {
		sublogger.Debug().Msg("Started querying validators signing infos")
		queryStart := time.Now()

		slashingClient := slashingtypes.NewQueryClient(grpcConn)
		signingInfosResponse, err := slashingClient.SigningInfos(
			ctx,
			&slashingtypes.QuerySigningInfosRequest{
				Pagination: &querytypes.PageRequest{
					Limit: Limit,
				},
			},
		)
		if err != nil {
			sublogger.Warn().
				Err(err).
				Msg("Could not get validators signing infos")
			return
		}

		sublogger.Debug().
			Float64("request-time", time.Since(queryStart).Seconds()).
			Msg("Finished querying validator signing infos")
		signingInfos = signingInfosResponse.Info
	})

	goSafe(&wg, func() {
		sublogger.Debug().Msg("Started querying staking params")
		queryStart := time.Now()

		stakingClient := stakingtypes.NewQueryClient(grpcConn)
		paramsResponse, err := stakingClient.Params(
			ctx,
			&stakingtypes.QueryParamsRequest{},
		)
		if err != nil {
			sublogger.Warn().
				Err(err).
				Msg("Could not get staking params")
			return
		}

		sublogger.Debug().
			Float64("request-time", time.Since(queryStart).Seconds()).
			Msg("Finished querying staking params")
		validatorSetLength = paramsResponse.Params.MaxValidators
	})

	wg.Wait()

	if validatorsErr != nil {
		return nil, validatorsErr
	}

	sublogger.Debug().
		Int("signingLength", len(signingInfos)).
		Int("validatorsLength", len(validators)).
		Msg("Validators info")

	for index, validator := range validators {
		moniker := validator.Description.Moniker
		moniker = sanitizeUTF8(moniker)

		// Исправление для validator.Tokens
		value, _ := new(big.Float).SetInt(validator.Tokens.BigInt()).Float64()
		validatorsTokensGauge.With(prometheus.Labels{
			"address": validator.OperatorAddress,
			"moniker": moniker,
			"denom":   Denom,
		}).Set(value / DenomCoefficient)

		validatorsStatusGauge.With(prometheus.Labels{
			"address": validator.OperatorAddress,
			"moniker": moniker,
		}).Set(float64(validator.Status))

		var jailed float64
		if validator.Jailed {
			jailed = 1
		} else {
			jailed = 0
		}
		validatorsJailedGauge.With(prometheus.Labels{
			"address": validator.OperatorAddress,
			"moniker": moniker,
		}).Set(jailed)

		// LegacyDec.BigInt() возвращает внутреннее представление ×10^18,
		// поэтому конвертируем через нормализованную строку.
		value, err := strconv.ParseFloat(validator.DelegatorShares.String(), 64)
		if err != nil {
			sublogger.Error().
				Str("address", validator.OperatorAddress).
				Err(err).
				Msg("Could not parse delegator shares")
			value = 0
		}
		validatorsDelegatorSharesGauge.With(prometheus.Labels{
			"address": validator.OperatorAddress,
			"moniker": moniker,
			"denom":   Denom,
		}).Set(value / DenomCoefficient)

		// Исправление для validator.MinSelfDelegation
		value, _ = new(big.Float).SetInt(validator.MinSelfDelegation.BigInt()).Float64()
		validatorsMinSelfDelegationGauge.With(prometheus.Labels{
			"address": validator.OperatorAddress,
			"moniker": moniker,
			"denom":   Denom,
		}).Set(value / DenomCoefficient)

		consAddr, err := consAddressFromValidator(validator, ConsensusNodePrefix)
		if err != nil {
			sublogger.Warn().
				Str("address", validator.OperatorAddress).
				Err(err).
				Msg("Could not derive validator consensus address")
			continue
		}

		var signingInfo slashingtypes.ValidatorSigningInfo
		found := false
		for _, signingInfoIterated := range signingInfos {
			if consAddr == signingInfoIterated.Address {
				found = true
				signingInfo = signingInfoIterated
				break
			}
		}

		if !found {
			sublogger.Debug().
				Str("address", validator.OperatorAddress).
				Msg("Could not get signing info for validator")
		} else if validator.Status == stakingtypes.Bonded {
			validatorsMissedBlocksGauge.With(prometheus.Labels{
				"address": validator.OperatorAddress,
				"moniker": moniker,
			}).Set(float64(signingInfo.MissedBlocksCounter))
		} else {
			sublogger.Trace().
				Str("address", validator.OperatorAddress).
				Msg("Validator is not active, not returning missed blocks amount.")
		}

		validatorsRankGauge.With(prometheus.Labels{
			"address": validator.OperatorAddress,
			"moniker": moniker,
		}).Set(float64(index + 1))

		if validatorSetLength != 0 {
			var active float64
			if index+1 <= int(validatorSetLength) {
				active = 1
			} else {
				active = 0
			}
			validatorsIsActiveGauge.With(prometheus.Labels{
				"address": validator.OperatorAddress,
				"moniker": moniker,
			}).Set(active)
		}
	}

	sublogger.Info().
		Str("endpoint", "/metrics/validators").
		Float64("collect-time", time.Since(requestStart).Seconds()).
		Msg("Metrics collected")

	return registry, nil
}

func sanitizeUTF8(input string) string {
	buf := &bytes.Buffer{}
	for _, runeValue := range input {
		if utf8.ValidRune(runeValue) {
			buf.WriteRune(runeValue)
		}
	}
	return buf.String()
}
