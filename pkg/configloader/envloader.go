package configloader

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type EnvVar struct {
	Value        string
	DefaultValue any
}

func GetEnv(key string, defaultValue any) *EnvVar {
	envVar := EnvVar{
		DefaultValue: defaultValue,
	}
	if value, exists := os.LookupEnv(key); exists {
		envVar.Value = value
	}
	return &envVar
}

func (env *EnvVar) Int() int {
	value := env.Value
	parsedInteger, err := strconv.Atoi(value)
	if err != nil {
		defaultValue, _ := anyToInt(env.DefaultValue)
		return defaultValue
	}
	return parsedInteger
}

func (env *EnvVar) Bool() bool {
	value := env.Value
	parsedBool, err := parseBool(value)
	if err != nil {
		defaultValue, _ := anyToBool(env.DefaultValue)
		return defaultValue
	}
	return parsedBool
}

func (env *EnvVar) String() string {
	value := env.Value
	parsedString := fmt.Sprint(value)
	return parsedString
}

func anyToInt(value any) (int, error) {
	var result int
	_, err := fmt.Sscan(fmt.Sprint(value), &result)
	return result, err
}

func anyToBool(value any) (bool, error) {
	str := fmt.Sprint(value)
	result, err := parseBool(str)
	return result, err
}

func parseBool(str string) (bool, error) {
	switch strings.ToLower(str) {
	case "true", "yes", "y", "1", "on":
		return true, nil
	case "false", "no", "n", "0", "off":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean string: %s", str)
	}
}
