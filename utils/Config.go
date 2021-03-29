package utils

import (
	"github.com/spf13/viper"
)

// ReadConfig returns a viper configuration object
func ReadConfig(configFileName string, defaults map[string]interface{}) (*viper.Viper, error) {

	v := viper.New()
	for key, val := range defaults {
		v.SetDefault(key, val)
	}
	v.SetConfigName(configFileName)
	v.AddConfigPath("config/")
	err := v.ReadInConfig()
	return v, err
}
