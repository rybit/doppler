package conf

import (
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"errors"

	"github.com/rybit/doppler/messaging"
	"github.com/rybit/doppler/redshift"
)

type Config struct {
	ReportSec int64 `mapstructure:"report_sec"`

	RedshiftConf redshift.RedshiftConfig `mapstructure:"redshift_conf"`
	NatsConf     messaging.NatsConfig    `mapstructure:"nats_conf"`
	LogConf      LoggingConfig           `mapstructure:"log_conf"`
}

// LoadConfig loads the config from a file if specified, otherwise from the environment
func LoadConfig(cmd *cobra.Command) (*Config, error) {
	viper.SetConfigType("json")

	err := viper.BindPFlags(cmd.Flags())
	if err != nil {
		return nil, err
	}

	viper.SetEnvPrefix("doppler")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	if configFile, _ := cmd.Flags().GetString("config"); configFile != "" {
		viper.SetConfigFile(configFile)
	} else {
		viper.SetConfigName("config")
		viper.AddConfigPath("./")
		viper.AddConfigPath("$HOME/.doppler/")
	}

	if err := viper.ReadInConfig(); err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	config := new(Config)

	if err := viper.Unmarshal(config); err != nil {
		return nil, err
	}

	return validate(config)
}

func validate(config *Config) (*Config, error) {
	if config.NatsConf.PoolSize == 0 {
		return nil, errors.New("Must provide at least one worker")
	}

	return config, nil
}
