package main

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"

	querytypes "github.com/cosmos/cosmos-sdk/types/query"
	distributiontypes "github.com/cosmos/cosmos-sdk/x/distribution/types"
	slashingtypes "github.com/cosmos/cosmos-sdk/x/slashing/types"
	stakingtypes "github.com/cosmos/cosmos-sdk/x/staking/types"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

func collectValidatorMetrics(ctx context.Context, grpcConn *grpc.ClientConn, address string) (*prometheus.Registry, error) {
	requestStart := time.Now()
	sublogger := log.With().
		Str("request-id", uuid.New().String()).
		Logger()

	validatorDelegationsGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "cosmos_validator_delegations",
			Help:        "Delegations of the Cosmos-based blockchain validator",
			ConstLabels: ConstLabels,
		},
		[]string{"address", "moniker", "denom", "delegated_by"},
	)

	validatorTokensGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "cosmos_validator_tokens",
			Help:        "Tokens of the Cosmos-based blockchain validator",
			ConstLabels: ConstLabels,
		},
		[]string{"address", "moniker", "denom"},
	)

	validatorDelegatorSharesGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "cosmos_validator_delegators_shares",
			Help:        "Delegators shares of the Cosmos-based blockchain validator",
			ConstLabels: ConstLabels,
		},
		[]string{"address", "moniker", "denom"},
	)

	validatorCommissionRateGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "cosmos_validator_commission_rate",
			Help:        "Commission rate of the Cosmos-based blockchain validator",
			ConstLabels: ConstLabels,
		},
		[]string{"address", "moniker"},
	)

	validatorCommissionGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "cosmos_validator_commission",
			Help:        "Commission of the Cosmos-based blockchain validator",
			ConstLabels: ConstLabels,
		},
		[]string{"address", "moniker", "denom"},
	)

	validatorRewardsGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "cosmos_validator_rewards",
			Help:        "Rewards of the Cosmos-based blockchain validator",
			ConstLabels: ConstLabels,
		},
		[]string{"address", "moniker", "denom"},
	)

	validatorUnbondingsGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "cosmos_validator_unbondings",
			Help:        "Unbondings of the Cosmos-based blockchain validator",
			ConstLabels: ConstLabels,
		},
		[]string{"address", "moniker", "denom", "unbonded_by"},
	)

	validatorRedelegationsGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "cosmos_validator_redelegations",
			Help:        "Redelegations of the Cosmos-based blockchain validator",
			ConstLabels: ConstLabels,
		},
		[]string{"address", "moniker", "denom", "redelegated_by", "redelegated_to"},
	)

	validatorMissedBlocksGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "cosmos_validator_missed_blocks",
			Help:        "Missed blocks of the Cosmos-based blockchain validator",
			ConstLabels: ConstLabels,
		},
		[]string{"address", "moniker"},
	)

	validatorRankGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "cosmos_validator_rank",
			Help:        "Rank of the Cosmos-based blockchain validator",
			ConstLabels: ConstLabels,
		},
		[]string{"address", "moniker"},
	)

	validatorIsActiveGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "cosmos_validator_active",
			Help:        "1 if the Cosmos-based blockchain validator is in active set, 0 if not",
			ConstLabels: ConstLabels,
		},
		[]string{"address", "moniker"},
	)

	validatorStatusGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "cosmos_validator_status",
			Help:        "Status of the Cosmos-based blockchain validator",
			ConstLabels: ConstLabels,
		},
		[]string{"address", "moniker"},
	)

	validatorJailedGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "cosmos_validator_jailed",
			Help:        "1 if the Cosmos-based blockchain validator is jailed, 0 if not",
			ConstLabels: ConstLabels,
		},
		[]string{"address", "moniker"},
	)

	registry := prometheus.NewRegistry()
	registry.MustRegister(validatorDelegationsGauge)
	registry.MustRegister(validatorTokensGauge)
	registry.MustRegister(validatorDelegatorSharesGauge)
	registry.MustRegister(validatorCommissionRateGauge)
	registry.MustRegister(validatorCommissionGauge)
	registry.MustRegister(validatorRewardsGauge)
	registry.MustRegister(validatorUnbondingsGauge)
	registry.MustRegister(validatorRedelegationsGauge)
	registry.MustRegister(validatorMissedBlocksGauge)
	registry.MustRegister(validatorRankGauge)
	registry.MustRegister(validatorIsActiveGauge)
	registry.MustRegister(validatorStatusGauge)
	registry.MustRegister(validatorJailedGauge)

	sublogger.Debug().
		Str("address", address).
		Msg("Started querying validator")
	validatorQueryStart := time.Now()

	stakingClient := stakingtypes.NewQueryClient(grpcConn)
	validatorResp, err := stakingClient.Validator(
		ctx,
		&stakingtypes.QueryValidatorRequest{ValidatorAddr: address},
	)
	if err != nil {
		return nil, fmt.Errorf("could not get validator %s: %w", address, err)
	}

	validator := validatorResp.Validator
	moniker := sanitizeUTF8(validator.Description.Moniker)

	sublogger.Debug().
		Str("address", address).
		Float64("request-time", time.Since(validatorQueryStart).Seconds()).
		Msg("Finished querying validator")

	if value, err := strconv.ParseFloat(validator.Tokens.String(), 64); err != nil {
		sublogger.Error().
			Str("address", address).
			Err(err).
			Msg("Could not parse validator tokens")
	} else {
		validatorTokensGauge.With(prometheus.Labels{
			"address": validator.OperatorAddress,
			"moniker": moniker,
			"denom":   Denom,
		}).Set(value / DenomCoefficient)
	}

	if value, err := strconv.ParseFloat(validator.DelegatorShares.String(), 64); err != nil {
		sublogger.Error().
			Str("address", address).
			Err(err).
			Msg("Could not parse delegator shares")
	} else {
		validatorDelegatorSharesGauge.With(prometheus.Labels{
			"address": validator.OperatorAddress,
			"moniker": moniker,
			"denom":   Denom,
		}).Set(value / DenomCoefficient)
	}

	if rate, err := strconv.ParseFloat(validator.Commission.CommissionRates.Rate.String(), 64); err != nil {
		sublogger.Error().
			Str("address", address).
			Err(err).
			Msg("Could not parse commission rate")
	} else {
		validatorCommissionRateGauge.With(prometheus.Labels{
			"address": validator.OperatorAddress,
			"moniker": moniker,
		}).Set(rate)
	}

	validatorStatusGauge.With(prometheus.Labels{
		"address": validator.OperatorAddress,
		"moniker": moniker,
	}).Set(float64(validator.Status))

	var jailed float64
	if validator.Jailed {
		jailed = 1
	}
	validatorJailedGauge.With(prometheus.Labels{
		"address": validator.OperatorAddress,
		"moniker": moniker,
	}).Set(jailed)

	var wg sync.WaitGroup

	goSafe(&wg, func() {
		if SkipValidatorDelegations {
			sublogger.Debug().
				Str("address", address).
				Msg("Skipping per-delegator metrics (--skip-validator-delegations)")
			return
		}
		if delegationsUnsupported.Load() {
			sublogger.Debug().
				Str("address", address).
				Msg("Skipping per-delegator metrics (chain refused the query earlier)")
			return
		}

		sublogger.Debug().
			Str("address", address).
			Msg("Started querying validator delegations")
		queryStart := time.Now()

		stakingClient := stakingtypes.NewQueryClient(grpcConn)
		delegations, err := paginateAll(ctx, Limit, MaxDelegations,
			func(ctx context.Context, page *querytypes.PageRequest) ([]stakingtypes.DelegationResponse, *querytypes.PageResponse, error) {
				res, err := stakingClient.ValidatorDelegations(
					ctx,
					&stakingtypes.QueryValidatorDelegationsRequest{
						ValidatorAddr: address,
						Pagination:    page,
					},
				)
				if err != nil {
					return nil, nil, err
				}
				return res.DelegationResponses, res.Pagination, nil
			})
		if err != nil {
			if isScanLimitError(err) {
				// Latch the metric off: every attempt forces the node to scan
				// a large part of the delegation store, which starves the
				// other queries in this same collection.
				if delegationsUnsupported.CompareAndSwap(false, true) {
					sublogger.Error().
						Str("address", address).
						Err(err).
						Msg("This chain refuses to serve validator delegations (store-scan limit); " +
							"cosmos_validator_delegations is now disabled for this process. " +
							"Add --skip-validator-delegations to silence this and skip the query entirely")
				}
				return
			}
			sublogger.Warn().
				Str("address", address).
				Err(err).
				Msg("Could not get validator delegations")
			return
		}

		sublogger.Debug().
			Str("address", address).
			Int("delegations", len(delegations)).
			Float64("request-time", time.Since(queryStart).Seconds()).
			Msg("Finished querying validator delegations")

		for _, delegation := range delegations {
			if value, err := strconv.ParseFloat(delegation.Balance.Amount.String(), 64); err != nil {
				sublogger.Error().
					Err(err).
					Str("address", address).
					Msg("Could not convert delegation entry")
			} else {
				validatorDelegationsGauge.With(prometheus.Labels{
					"moniker":      moniker,
					"address":      delegation.Delegation.ValidatorAddress,
					"denom":        Denom,
					"delegated_by": delegation.Delegation.DelegatorAddress,
				}).Set(value / DenomCoefficient)
			}
		}
	})

	goSafe(&wg, func() {
		sublogger.Debug().
			Str("address", address).
			Msg("Started querying validator commission")
		queryStart := time.Now()

		distributionClient := distributiontypes.NewQueryClient(grpcConn)
		distributionRes, err := distributionClient.ValidatorCommission(
			ctx,
			&distributiontypes.QueryValidatorCommissionRequest{ValidatorAddress: address},
		)
		if err != nil {
			sublogger.Warn().
				Str("address", address).
				Err(err).
				Msg("Could not get validator commission")
			return
		}

		sublogger.Debug().
			Str("address", address).
			Float64("request-time", time.Since(queryStart).Seconds()).
			Msg("Finished querying validator commission")

		for _, commission := range distributionRes.Commission.Commission {
			if value, err := strconv.ParseFloat(commission.Amount.String(), 64); err != nil {
				sublogger.Error().
					Err(err).
					Str("address", address).
					Msg("Could not parse validator commission")
			} else {
				validatorCommissionGauge.With(prometheus.Labels{
					"address": address,
					"moniker": moniker,
					"denom":   Denom,
				}).Set(value / DenomCoefficient)
			}
		}
	})

	goSafe(&wg, func() {
		sublogger.Debug().
			Str("address", address).
			Msg("Started querying validator rewards")
		queryStart := time.Now()

		distributionClient := distributiontypes.NewQueryClient(grpcConn)
		distributionRes, err := distributionClient.ValidatorOutstandingRewards(
			ctx,
			&distributiontypes.QueryValidatorOutstandingRewardsRequest{ValidatorAddress: address},
		)
		if err != nil {
			sublogger.Warn().
				Str("address", address).
				Err(err).
				Msg("Could not get validator rewards")
			return
		}

		sublogger.Debug().
			Str("address", address).
			Float64("request-time", time.Since(queryStart).Seconds()).
			Msg("Finished querying validator rewards")

		for _, reward := range distributionRes.Rewards.Rewards {
			if value, err := strconv.ParseFloat(reward.Amount.String(), 64); err != nil {
				sublogger.Error().
					Str("address", address).
					Err(err).
					Msg("Could not parse reward")
			} else {
				validatorRewardsGauge.With(prometheus.Labels{
					"address": address,
					"moniker": moniker,
					"denom":   Denom,
				}).Set(value / DenomCoefficient)
			}
		}
	})

	goSafe(&wg, func() {
		sublogger.Debug().
			Str("address", address).
			Msg("Started querying validator unbonding delegations")
		queryStart := time.Now()

		stakingClient := stakingtypes.NewQueryClient(grpcConn)
		stakingRes, err := stakingClient.ValidatorUnbondingDelegations(
			ctx,
			&stakingtypes.QueryValidatorUnbondingDelegationsRequest{ValidatorAddr: address},
		)
		if err != nil {
			sublogger.Warn().
				Str("address", address).
				Err(err).
				Msg("Could not get validator unbonding delegations")
			return
		}

		sublogger.Debug().
			Str("address", address).
			Float64("request-time", time.Since(queryStart).Seconds()).
			Msg("Finished querying validator unbonding delegations")

		for _, unbonding := range stakingRes.UnbondingResponses {
			var sum float64
			for _, entry := range unbonding.Entries {
				if value, err := strconv.ParseFloat(entry.Balance.String(), 64); err != nil {
					sublogger.Error().
						Err(err).
						Str("address", address).
						Msg("Could not convert unbonding delegation entry")
				} else {
					sum += value
				}
			}

			validatorUnbondingsGauge.With(prometheus.Labels{
				"address":     unbonding.ValidatorAddress,
				"moniker":     moniker,
				"denom":       Denom,
				"unbonded_by": unbonding.DelegatorAddress,
			}).Set(sum / DenomCoefficient)
		}
	})

	goSafe(&wg, func() {
		sublogger.Debug().
			Str("address", address).
			Msg("Started querying validator redelegations")
		queryStart := time.Now()

		stakingClient := stakingtypes.NewQueryClient(grpcConn)
		stakingRes, err := stakingClient.Redelegations(
			ctx,
			&stakingtypes.QueryRedelegationsRequest{SrcValidatorAddr: address},
		)
		if err != nil {
			sublogger.Warn().
				Str("address", address).
				Err(err).
				Msg("Could not get redelegations")
			return
		}

		sublogger.Debug().
			Str("address", address).
			Float64("request-time", time.Since(queryStart).Seconds()).
			Msg("Finished querying validator redelegations")

		for _, redelegation := range stakingRes.RedelegationResponses {
			var sum float64
			for _, entry := range redelegation.Entries {
				if value, err := strconv.ParseFloat(entry.Balance.String(), 64); err != nil {
					sublogger.Error().
						Err(err).
						Str("address", address).
						Msg("Could not convert redelegation entry")
				} else {
					sum += value
				}
			}

			validatorRedelegationsGauge.With(prometheus.Labels{
				"address":        redelegation.Redelegation.ValidatorSrcAddress,
				"moniker":        moniker,
				"denom":          Denom,
				"redelegated_by": redelegation.Redelegation.DelegatorAddress,
				"redelegated_to": redelegation.Redelegation.ValidatorDstAddress,
			}).Set(sum / DenomCoefficient)
		}
	})

	goSafe(&wg, func() {
		sublogger.Debug().
			Str("address", address).
			Msg("Started querying validator signing info")

		consAddr, err := consAddressFromValidator(validator, ConsensusNodePrefix)
		if err != nil {
			sublogger.Warn().
				Str("address", validator.OperatorAddress).
				Err(err).
				Msg("Could not derive validator consensus address, skipping missed blocks")
			return
		}

		slashingClient := slashingtypes.NewQueryClient(grpcConn)
		slashingRes, err := slashingClient.SigningInfo(
			ctx,
			&slashingtypes.QuerySigningInfoRequest{ConsAddress: consAddr},
		)
		if err != nil {
			sublogger.Warn().
				Str("address", validator.OperatorAddress).
				Str("cons-address", consAddr).
				Err(err).
				Msg("Could not get signing info for validator")
			return
		}

		if validator.Status == stakingtypes.Bonded {
			validatorMissedBlocksGauge.With(prometheus.Labels{
				"address": validator.OperatorAddress,
				"moniker": moniker,
			}).Set(float64(slashingRes.ValSigningInfo.MissedBlocksCounter))
		} else {
			sublogger.Trace().
				Str("address", validator.OperatorAddress).
				Msg("Validator is not active, not returning missed blocks amount.")
		}
	})

	goSafe(&wg, func() {
		sublogger.Debug().
			Str("address", address).
			Msg("Started querying validator rank and active status")
		queryStart := time.Now()

		stakingClient := stakingtypes.NewQueryClient(grpcConn)
		validators, err := paginateAll(ctx, Limit, 0,
			func(ctx context.Context, page *querytypes.PageRequest) ([]stakingtypes.Validator, *querytypes.PageResponse, error) {
				res, err := stakingClient.Validators(ctx, &stakingtypes.QueryValidatorsRequest{Pagination: page})
				if err != nil {
					return nil, nil, err
				}
				return res.Validators, res.Pagination, nil
			})
		if err != nil {
			sublogger.Warn().
				Str("address", address).
				Err(err).
				Msg("Could not get validators list")
			return
		}

		sort.Slice(validators, func(i, j int) bool {
			return validators[i].DelegatorShares.GT(validators[j].DelegatorShares)
		})

		var validatorRank int
		for index, validatorIterated := range validators {
			if validatorIterated.OperatorAddress == validator.OperatorAddress {
				validatorRank = index + 1
				break
			}
		}

		if validatorRank == 0 {
			sublogger.Warn().
				Str("address", address).
				Msg("Could not find validator in validators list")
			return
		}

		validatorRankGauge.With(prometheus.Labels{
			"moniker": moniker,
			"address": validator.OperatorAddress,
		}).Set(float64(validatorRank))

		paramsRes, err := stakingClient.Params(
			ctx,
			&stakingtypes.QueryParamsRequest{},
		)
		if err != nil {
			sublogger.Warn().
				Str("address", address).
				Err(err).
				Msg("Could not get staking params")
			return
		}

		var active float64
		if validatorRank <= int(paramsRes.Params.MaxValidators) {
			active = 1
		}

		validatorIsActiveGauge.With(prometheus.Labels{
			"address": validator.OperatorAddress,
			"moniker": moniker,
		}).Set(active)

		sublogger.Debug().
			Str("address", address).
			Float64("request-time", time.Since(queryStart).Seconds()).
			Msg("Finished querying validator rank and active status")
	})

	wg.Wait()

	sublogger.Info().
		Str("endpoint", "/metrics/validator?address="+address).
		Float64("collect-time", time.Since(requestStart).Seconds()).
		Msg("Metrics collected")

	return registry, nil
}
