package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/cosmos/cosmos-sdk/client/grpc/cmtservice"
	sdk "github.com/cosmos/cosmos-sdk/types"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	stakingtypes "github.com/cosmos/cosmos-sdk/x/staking/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var (
	ConfigPath string

	Denom         string
	ListenAddress string
	NodeAddress   string
	TendermintRPC string
	LogLevel      string
	JsonOutput    bool
	Limit         uint64

	Prefix                    string
	AccountPrefix             string
	AccountPubkeyPrefix       string
	ValidatorPrefix           string
	ValidatorPubkeyPrefix     string
	ConsensusNodePrefix       string
	ConsensusNodePubkeyPrefix string

	ChainID          string
	ConstLabels      map[string]string
	DenomCoefficient float64
	DenomExponent    uint64

	RefreshInterval time.Duration
	GRPCTimeout     time.Duration
)

var log = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).With().Timestamp().Logger()

var rootCmd = &cobra.Command{
	Use:  "cosmos-exporter",
	Long: "Scrape the data about the validators set, specific validators or wallets in the Cosmos network.",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if ConfigPath == "" {
			setBechPrefixes(cmd)
			return nil
		}

		viper.SetConfigFile(ConfigPath)
		if err := viper.ReadInConfig(); err != nil {
			if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
				log.Info().Err(err).Msg("Error reading config file")
				return err
			}
		}

		cmd.Flags().VisitAll(func(f *pflag.Flag) {
			if !f.Changed && viper.IsSet(f.Name) {
				val := viper.Get(f.Name)
				if err := cmd.Flags().Set(f.Name, fmt.Sprintf("%v", val)); err != nil {
					log.Fatal().Err(err).Msg("Could not set flag")
				}
			}
		})

		setBechPrefixes(cmd)
		return nil
	},
	Run: Execute,
}

func setBechPrefixes(cmd *cobra.Command) {
	if flag, err := cmd.Flags().GetString("bech-account-prefix"); flag != "" && err == nil {
		AccountPrefix = flag
	} else {
		AccountPrefix = Prefix
	}

	if flag, err := cmd.Flags().GetString("bech-account-pubkey-prefix"); flag != "" && err == nil {
		AccountPubkeyPrefix = flag
	} else {
		AccountPubkeyPrefix = Prefix + "pub"
	}

	if flag, err := cmd.Flags().GetString("bech-validator-prefix"); flag != "" && err == nil {
		ValidatorPrefix = flag
	} else {
		ValidatorPrefix = Prefix + "valoper"
	}

	if flag, err := cmd.Flags().GetString("bech-validator-pubkey-prefix"); flag != "" && err == nil {
		ValidatorPubkeyPrefix = flag
	} else {
		ValidatorPubkeyPrefix = Prefix + "valoperpub"
	}

	if flag, err := cmd.Flags().GetString("bech-consensus-node-prefix"); flag != "" && err == nil {
		ConsensusNodePrefix = flag
	} else {
		ConsensusNodePrefix = Prefix + "valcons"
	}

	if flag, err := cmd.Flags().GetString("bech-consensus-node-pubkey-prefix"); flag != "" && err == nil {
		ConsensusNodePubkeyPrefix = flag
	} else {
		ConsensusNodePubkeyPrefix = Prefix + "valconspub"
	}
}

func Execute(cmd *cobra.Command, args []string) {
	logLevel, err := zerolog.ParseLevel(LogLevel)
	if err != nil {
		log.Fatal().Err(err).Msg("Could not parse log level")
	}

	if JsonOutput {
		log = zerolog.New(os.Stdout).With().Timestamp().Logger()
	}

	zerolog.SetGlobalLevel(logLevel)

	log.Info().
		Str("--bech-account-prefix", AccountPrefix).
		Str("--bech-account-pubkey-prefix", AccountPubkeyPrefix).
		Str("--bech-validator-prefix", ValidatorPrefix).
		Str("--bech-validator-pubkey-prefix", ValidatorPubkeyPrefix).
		Str("--bech-consensus-node-prefix", ConsensusNodePrefix).
		Str("--bech-consensus-node-pubkey-prefix", ConsensusNodePubkeyPrefix).
		Str("--denom", Denom).
		Str("--denom-coefficient", fmt.Sprintf("%f", DenomCoefficient)).
		Str("--denom-exponent", fmt.Sprintf("%d", DenomExponent)).
		Str("--listen-address", ListenAddress).
		Str("--node", NodeAddress).
		Str("--log-level", LogLevel).
		Msg("Started with following parameters")

	config := sdk.GetConfig()
	config.SetBech32PrefixForAccount(AccountPrefix, AccountPubkeyPrefix)
	config.SetBech32PrefixForValidator(ValidatorPrefix, ValidatorPubkeyPrefix)
	config.SetBech32PrefixForConsensusNode(ConsensusNodePrefix, ConsensusNodePubkeyPrefix)
	config.Seal()

	grpcConn, err := grpc.NewClient(
		NodeAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatal().Err(err).Msg("Could not connect to gRPC node")
	}
	defer grpcConn.Close()

	setChainID(grpcConn)
	setDenom(grpcConn)

	cache := NewMetricsCache(RefreshInterval, GRPCTimeout)

	http.HandleFunc("/metrics/wallet", func(w http.ResponseWriter, r *http.Request) {
		address := r.URL.Query().Get("address")
		myAddress, err := sdk.AccAddressFromBech32(address)
		if err != nil {
			log.Error().Str("address", address).Err(err).Msg("Could not parse wallet address")
			http.Error(w, fmt.Sprintf("invalid wallet address %q: %v", address, err), http.StatusBadRequest)
			return
		}
		cache.Serve(w, r, "wallet?address="+myAddress.String(), func(ctx context.Context) ([]byte, error) {
			registry, err := collectWalletMetrics(ctx, grpcConn, myAddress.String())
			if err != nil {
				return nil, err
			}
			return renderRegistry(registry)
		})
	})

	http.HandleFunc("/metrics/validator", func(w http.ResponseWriter, r *http.Request) {
		address := r.URL.Query().Get("address")
		myAddress, err := sdk.ValAddressFromBech32(address)
		if err != nil {
			log.Error().Str("address", address).Err(err).Msg("Could not parse validator address")
			http.Error(w, fmt.Sprintf("invalid validator address %q: %v", address, err), http.StatusBadRequest)
			return
		}
		cache.Serve(w, r, "validator?address="+myAddress.String(), func(ctx context.Context) ([]byte, error) {
			registry, err := collectValidatorMetrics(ctx, grpcConn, myAddress.String())
			if err != nil {
				return nil, err
			}
			return renderRegistry(registry)
		})
	})

	http.HandleFunc("/metrics/validators", cachedMetricsHandler(cache, "validators", func(ctx context.Context) (*prometheus.Registry, error) {
		return collectValidatorsMetrics(ctx, grpcConn)
	}))

	http.HandleFunc("/metrics/params", cachedMetricsHandler(cache, "params", func(ctx context.Context) (*prometheus.Registry, error) {
		return collectParamsMetrics(ctx, grpcConn)
	}))

	http.HandleFunc("/metrics/general", cachedMetricsHandler(cache, "general", func(ctx context.Context) (*prometheus.Registry, error) {
		return collectGeneralMetrics(ctx, grpcConn)
	}))

	log.Info().Str("address", ListenAddress).Msg("Listening")
	err = http.ListenAndServe(ListenAddress, nil)
	if err != nil {
		log.Fatal().Err(err).Msg("Could not start application")
	}
}

// cachedMetricsHandler wraps a parameterless collect function into an HTTP
// handler that serves through the metrics cache.
func cachedMetricsHandler(cache *MetricsCache, key string, collect func(ctx context.Context) (*prometheus.Registry, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cache.Serve(w, r, key, func(ctx context.Context) ([]byte, error) {
			registry, err := collect(ctx)
			if err != nil {
				return nil, err
			}
			return renderRegistry(registry)
		})
	}
}

// setChainID resolves the chain ID from Tendermint RPC, falling back to the
// gRPC tendermint service, and finally to "unknown" so metrics stay labeled
// even if the node was unreachable at startup.
func setChainID(grpcConn *grpc.ClientConn) {
	defer func() {
		ConstLabels = prometheus.Labels{
			"chain_id": ChainID,
		}
	}()

	if chainID := chainIDFromTendermintRPC(); chainID != "" {
		ChainID = chainID
		log.Info().Str("chain_id", ChainID).Msg("Got chain ID from Tendermint RPC")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), GRPCTimeout)
	defer cancel()

	serviceClient := cmtservice.NewServiceClient(grpcConn)
	nodeInfo, err := serviceClient.GetNodeInfo(ctx, &cmtservice.GetNodeInfoRequest{})
	if err == nil && nodeInfo.DefaultNodeInfo != nil && nodeInfo.DefaultNodeInfo.Network != "" {
		ChainID = nodeInfo.DefaultNodeInfo.Network
		log.Info().Str("chain_id", ChainID).Msg("Got chain ID from gRPC node info")
		return
	}

	log.Warn().Err(err).Msg("Could not determine chain ID from RPC or gRPC, using \"unknown\"")
	ChainID = "unknown"
}

func chainIDFromTendermintRPC() string {
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	resp, err := client.Get(fmt.Sprintf("%s/status", TendermintRPC))
	if err != nil {
		log.Warn().Err(err).Msg("Could not query node status via Tendermint RPC")
		return ""
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Warn().Err(err).Msg("Could not read Tendermint RPC status response")
		return ""
	}

	var result struct {
		Result struct {
			NodeInfo struct {
				Network string `json:"network"`
			} `json:"node_info"`
		} `json:"result"`
	}

	if err := json.Unmarshal(body, &result); err != nil {
		log.Warn().Err(err).Msg("Could not parse Tendermint RPC status response")
		return ""
	}

	return result.Result.NodeInfo.Network
}

func setDenom(grpcConn *grpc.ClientConn) {
	if isUserProvidedAndHandled := checkAndHandleDenomInfoProvidedByUser(); isUserProvidedAndHandled {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), GRPCTimeout)
	defer cancel()

	bankClient := banktypes.NewQueryClient(grpcConn)
	denoms, err := bankClient.DenomsMetadata(
		ctx,
		&banktypes.QueryDenomsMetadataRequest{},
	)
	if err != nil || len(denoms.Metadatas) == 0 {
		// Кастомные сети (Sei) часто не публикуют DenomsMetadata через bank —
		// пробуем bond denom из staking params, прежде чем сдаваться.
		log.Warn().Err(err).Msg("No denom metadata on chain, falling back to staking bond denom")
		setDenomFromStakingParams(grpcConn)
		return
	}

	metadata := denoms.Metadatas[0]
	if Denom == "" {
		Denom = metadata.Display
	}

	for _, unit := range metadata.DenomUnits {
		log.Debug().
			Str("denom", unit.Denom).
			Uint32("exponent", unit.Exponent).
			Msg("Denom info")
		if unit.Denom == Denom {
			DenomCoefficient = math.Pow10(int(unit.Exponent))
			log.Info().
				Str("denom", Denom).
				Float64("coefficient", DenomCoefficient).
				Msg("Got denom info")
			return
		}
	}

	log.Fatal().Msg("Could not find the denom info")
}

func setDenomFromStakingParams(grpcConn *grpc.ClientConn) {
	ctx, cancel := context.WithTimeout(context.Background(), GRPCTimeout)
	defer cancel()

	stakingClient := stakingtypes.NewQueryClient(grpcConn)
	paramsResp, err := stakingClient.Params(ctx, &stakingtypes.QueryParamsRequest{})
	if err != nil || paramsResp.Params.BondDenom == "" {
		log.Fatal().Err(err).Msg("Could not determine denom. Run the binary with --denom and --denom-exponent to set them manually.")
	}

	Denom = paramsResp.Params.BondDenom
	DenomCoefficient = 1
	log.Warn().
		Str("denom", Denom).
		Msg("Using staking bond denom with coefficient 1. Set --denom and --denom-exponent explicitly for correct value scaling.")
}

func checkAndHandleDenomInfoProvidedByUser() bool {
	if Denom != "" {
		if DenomCoefficient != 1 && DenomExponent != 0 {
			log.Fatal().Msg("denom-coefficient and denom-exponent are both provided. Must provide only one")
		}

		if DenomCoefficient != 1 {
			log.Info().
				Str("denom", Denom).
				Float64("coefficient", DenomCoefficient).
				Msg("Using provided denom and coefficient.")
			return true
		}

		if DenomExponent != 0 {
			DenomCoefficient = math.Pow10(int(DenomExponent))
			log.Info().
				Str("denom", Denom).
				Uint64("exponent", DenomExponent).
				Float64("calculated coefficient", DenomCoefficient).
				Msg("Using provided denom and denom exponent and calculating coefficient.")
			return true
		}

		return false
	}

	return false
}

func main() {
	rootCmd.PersistentFlags().StringVar(&ConfigPath, "config", "", "Config file path")
	rootCmd.PersistentFlags().StringVar(&Denom, "denom", "", "Cosmos coin denom")
	rootCmd.PersistentFlags().Float64Var(&DenomCoefficient, "denom-coefficient", 1, "Denom coefficient")
	rootCmd.PersistentFlags().Uint64Var(&DenomExponent, "denom-exponent", 0, "Denom exponent")
	rootCmd.PersistentFlags().StringVar(&ListenAddress, "listen-address", ":9300", "The address this exporter would listen on")
	rootCmd.PersistentFlags().StringVar(&NodeAddress, "node", "localhost:9090", "RPC node address")
	rootCmd.PersistentFlags().StringVar(&LogLevel, "log-level", "info", "Logging level")
	rootCmd.PersistentFlags().Uint64Var(&Limit, "limit", 1000, "Pagination limit for gRPC requests")
	rootCmd.PersistentFlags().StringVar(&TendermintRPC, "tendermint-rpc", "http://localhost:26657", "Tendermint RPC address")
	rootCmd.PersistentFlags().BoolVar(&JsonOutput, "json", false, "Output logs as JSON")
	rootCmd.PersistentFlags().DurationVar(&RefreshInterval, "refresh-interval", 30*time.Second, "How often metrics may be re-collected from the node; scrapes in between are served from cache")
	rootCmd.PersistentFlags().DurationVar(&GRPCTimeout, "grpc-timeout", 15*time.Second, "Timeout for collecting metrics from the node")

	rootCmd.PersistentFlags().StringVar(&Prefix, "bech-prefix", "persistence", "Bech32 global prefix")
	rootCmd.PersistentFlags().StringVar(&AccountPrefix, "bech-account-prefix", "", "Bech32 account prefix")
	rootCmd.PersistentFlags().StringVar(&AccountPubkeyPrefix, "bech-account-pubkey-prefix", "", "Bech32 pubkey account prefix")
	rootCmd.PersistentFlags().StringVar(&ValidatorPrefix, "bech-validator-prefix", "", "Bech32 validator prefix")
	rootCmd.PersistentFlags().StringVar(&ValidatorPubkeyPrefix, "bech-validator-pubkey-prefix", "", "Bech32 pubkey validator prefix")
	rootCmd.PersistentFlags().StringVar(&ConsensusNodePrefix, "bech-consensus-node-prefix", "", "Bech32 consensus node prefix")
	rootCmd.PersistentFlags().StringVar(&ConsensusNodePubkeyPrefix, "bech-consensus-node-pubkey-prefix", "", "Bech32 pubkey consensus node prefix")

	if err := rootCmd.Execute(); err != nil {
		log.Fatal().Err(err).Msg("Could not start application")
	}
}
