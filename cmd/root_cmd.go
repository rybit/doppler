package cmd

import (
	"github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/rybit/doppler/conf"
)

var rootCmd = &cobra.Command{
	Short: "elastinat",
	Long:  "elastinat",
	//Run:   run,
}

func RootCmd() *cobra.Command {
	rootCmd.PersistentFlags().StringP("config", "c", "", "a config file to use")
	rootCmd.AddCommand(versionCmd, ingestCmd)

	return rootCmd
}

func start(cmd *cobra.Command) (*conf.Config, *logrus.Entry) {
	config, err := conf.LoadConfig(cmd)
	if err != nil {
		logrus.Fatalf("Failed to load configuation: %v", err)
	}

	log, err := conf.ConfigureLogging(&config.LogConf)
	if err != nil {
		logrus.Fatal("Failed to configure logging")
	}

	return config, log.WithField("version", Version)
}
