package main

import (
	"context"
	"fmt"
	"math/big"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	distributiontypes "github.com/cosmos/cosmos-sdk/x/distribution/types"
	minttypes "github.com/cosmos/cosmos-sdk/x/mint/types"
	stakingtypes "github.com/cosmos/cosmos-sdk/x/staking/types"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

func collectGeneralMetrics(ctx context.Context, grpcConn *grpc.ClientConn) (*prometheus.Registry, error) {
	requestStart := time.Now()

	sublogger := log.With().
		Str("request-id", uuid.New().String()).
		Logger()

	generalBondedTokensGauge := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "cosmos_general_bonded_tokens",
			Help:        "Bonded tokens",
			ConstLabels: ConstLabels,
		},
	)

	generalNotBondedTokensGauge := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "cosmos_general_not_bonded_tokens",
			Help:        "Not bonded tokens",
			ConstLabels: ConstLabels,
		},
	)

	generalCommunityPoolGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "cosmos_general_community_pool",
			Help:        "Community pool",
			ConstLabels: ConstLabels,
		},
		[]string{"denom"},
	)

	generalSupplyTotalGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "cosmos_general_supply_total",
			Help:        "Total supply",
			ConstLabels: ConstLabels,
		},
		[]string{"denom"},
	)

	generalInflationGauge := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "cosmos_general_inflation",
			Help:        "Inflation rate",
			ConstLabels: ConstLabels,
		},
	)

	generalAnnualProvisions := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "cosmos_general_annual_provisions",
			Help:        "Annual provisions",
			ConstLabels: ConstLabels,
		},
		[]string{"denoms"},
	)

	registry := prometheus.NewRegistry()
	registry.MustRegister(generalBondedTokensGauge)
	registry.MustRegister(generalNotBondedTokensGauge)
	registry.MustRegister(generalCommunityPoolGauge)
	registry.MustRegister(generalSupplyTotalGauge)
	registry.MustRegister(generalInflationGauge)
	registry.MustRegister(generalAnnualProvisions)

	var wg sync.WaitGroup
	var successfulQueries atomic.Int64

	goSafe(&wg, func() {
		sublogger.Debug().Msg("Started querying staking pool")
		queryStart := time.Now()

		stakingClient := stakingtypes.NewQueryClient(grpcConn)
		response, err := stakingClient.Pool(
			ctx,
			&stakingtypes.QueryPoolRequest{},
		)
		if err != nil {
			sublogger.Warn().Err(err).Msg("Could not get staking pool")
			return
		}
		successfulQueries.Add(1)

		sublogger.Debug().
			Float64("request-time", time.Since(queryStart).Seconds()).
			Msg("Finished querying staking pool")

		// Int64() паникует при переполнении (18-decimal деномы), поэтому
		// конвертируем через big.Float. Значения намеренно остаются в
		// микроединицах — дашборды завязаны на это (см. GOTCHAS).
		bondedTokens, _ := new(big.Float).SetInt(response.Pool.BondedTokens.BigInt()).Float64()
		notBondedTokens, _ := new(big.Float).SetInt(response.Pool.NotBondedTokens.BigInt()).Float64()
		generalBondedTokensGauge.Set(bondedTokens)
		generalNotBondedTokensGauge.Set(notBondedTokens)
	})

	goSafe(&wg, func() {
		sublogger.Debug().Msg("Started querying distribution community pool")
		queryStart := time.Now()

		distributionClient := distributiontypes.NewQueryClient(grpcConn)
		response, err := distributionClient.CommunityPool(
			ctx,
			&distributiontypes.QueryCommunityPoolRequest{},
		)
		if err != nil {
			sublogger.Warn().Err(err).Msg("Could not get distribution community pool")
			return
		}
		successfulQueries.Add(1)

		sublogger.Debug().
			Float64("request-time", time.Since(queryStart).Seconds()).
			Msg("Finished querying distribution community pool")

		for _, coin := range response.Pool {
			if value, err := strconv.ParseFloat(coin.Amount.String(), 64); err != nil {
				sublogger.Error().
					Err(err).
					Msg("Could not get community pool coin")
			} else {
				generalCommunityPoolGauge.With(prometheus.Labels{
					"denom": coin.Denom,
				}).Set(value / DenomCoefficient)
			}
		}
	})

	goSafe(&wg, func() {
		sublogger.Debug().Msg("Started querying bank total supply")
		queryStart := time.Now()

		bankClient := banktypes.NewQueryClient(grpcConn)
		response, err := bankClient.TotalSupply(
			ctx,
			&banktypes.QueryTotalSupplyRequest{},
		)
		if err != nil {
			sublogger.Warn().Err(err).Msg("Could not get bank total supply")
			return
		}
		successfulQueries.Add(1)

		sublogger.Debug().
			Float64("request-time", time.Since(queryStart).Seconds()).
			Msg("Finished querying bank total supply")

		for _, coin := range response.Supply {
			if value, err := strconv.ParseFloat(coin.Amount.String(), 64); err != nil {
				sublogger.Error().
					Err(err).
					Msg("Could not get total supply")
			} else {
				generalSupplyTotalGauge.With(prometheus.Labels{
					"denom": coin.Denom,
				}).Set(value / DenomCoefficient)
			}
		}
	})

	goSafe(&wg, func() {
		sublogger.Debug().Msg("Started querying inflation")
		queryStart := time.Now()

		mintClient := minttypes.NewQueryClient(grpcConn)
		response, err := mintClient.Inflation(
			ctx,
			&minttypes.QueryInflationRequest{},
		)
		if err != nil {
			// Кастомные mint-модули (Sei) не отдают стандартную инфляцию —
			// это ожидаемая деградация, а не сбой экспортера.
			sublogger.Warn().Err(err).Msg("Could not get inflation (custom mint module?)")
			return
		}
		successfulQueries.Add(1)

		sublogger.Debug().
			Float64("request-time", time.Since(queryStart).Seconds()).
			Msg("Finished querying inflation")

		if value, err := strconv.ParseFloat(response.Inflation.String(), 64); err != nil {
			sublogger.Error().
				Err(err).
				Msg("Could not parse inflation")
		} else {
			generalInflationGauge.Set(value)
		}
	})

	goSafe(&wg, func() {
		sublogger.Debug().Msg("Started querying annual provisions")
		queryStart := time.Now()

		mintClient := minttypes.NewQueryClient(grpcConn)
		response, err := mintClient.AnnualProvisions(
			ctx,
			&minttypes.QueryAnnualProvisionsRequest{},
		)
		if err != nil {
			sublogger.Warn().Err(err).Msg("Could not get annual provisions (custom mint module?)")
			return
		}
		successfulQueries.Add(1)

		sublogger.Debug().
			Float64("request-time", time.Since(queryStart).Seconds()).
			Msg("Finished querying annual provisions")

		if value, err := strconv.ParseFloat(response.AnnualProvisions.String(), 64); err != nil {
			sublogger.Error().
				Err(err).
				Msg("Could not parse annual provisions")
		} else {
			generalAnnualProvisions.With(prometheus.Labels{
				"denom": Denom,
			}).Set(value / DenomCoefficient)
		}
	})

	wg.Wait()

	if successfulQueries.Load() == 0 {
		return nil, fmt.Errorf("all general metric queries failed, node unreachable?")
	}

	sublogger.Info().
		Str("endpoint", "/metrics/general").
		Float64("collect-time", time.Since(requestStart).Seconds()).
		Msg("Metrics collected")

	return registry, nil
}
