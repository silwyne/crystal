package configloader

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type EnvVar struct {
	key          string
	value        string
	defaultValue any
	isSet        bool
}

func GetEnv(key string, defaultValue any) *EnvVar {
	value, exists := os.LookupEnv(key)
	return &EnvVar{
		key:          key,
		value:        value,
		defaultValue: defaultValue,
		isSet:        exists && value != "", // Consider empty as not set
	}
}

func (env *EnvVar) Int() int {
	if !env.isSet {
		return anyToIntWithDefault(env.defaultValue, 0)
	}

	parsedInt, err := strconv.Atoi(env.value)
	if err != nil {
		return anyToIntWithDefault(env.defaultValue, 0)
	}
	return parsedInt
}

func (env *EnvVar) Int64() int64 {
	if !env.isSet {
		return anyToInt64WithDefault(env.defaultValue, 0)
	}

	parsedInt, err := strconv.ParseInt(env.value, 10, 64)
	if err != nil {
		return anyToInt64WithDefault(env.defaultValue, 0)
	}
	return parsedInt
}

func (env *EnvVar) Bool() bool {
	if !env.isSet {
		return anyToBoolWithDefault(env.defaultValue, false)
	}

	parsedBool, err := parseBool(env.value)
	if err != nil {
		return anyToBoolWithDefault(env.defaultValue, false)
	}
	return parsedBool
}

func (env *EnvVar) String() string {
	if !env.isSet && env.defaultValue != nil {
		return fmt.Sprint(env.defaultValue)
	}
	return env.value
}

func (env *EnvVar) Float64() float64 {
	if !env.isSet {
		return anyToFloat64WithDefault(env.defaultValue, 0.0)
	}

	parsedFloat, err := strconv.ParseFloat(env.value, 64)
	if err != nil {
		return anyToFloat64WithDefault(env.defaultValue, 0.0)
	}
	return parsedFloat
}

func (env *EnvVar) Duration() time.Duration {
	if !env.isSet {
		if defDuration, ok := env.defaultValue.(time.Duration); ok {
			return defDuration
		}
		if defStr, ok := env.defaultValue.(string); ok {
			if d, err := time.ParseDuration(defStr); err == nil {
				return d
			}
		}
		return 0
	}

	if d, err := time.ParseDuration(env.value); err == nil {
		return d
	}

	// Try to parse as seconds (common pattern)
	if seconds, err := strconv.ParseInt(env.value, 10, 64); err == nil {
		return time.Duration(seconds) * time.Second
	}

	return 0
}

func (env *EnvVar) StringSlice(sep string) []string {
	if !env.isSet {
		if defSlice, ok := env.defaultValue.([]string); ok {
			return defSlice
		}
		return []string{}
	}

	if env.value == "" {
		return []string{}
	}

	return strings.Split(env.value, sep)
}

// Helper functions
func anyToIntWithDefault(value any, def int) int {
	if value == nil {
		return def
	}

	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	case string:
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	case bool:
		if v {
			return 1
		}
		return 0
	default:
		// Last resort: convert to string and parse
		if i, err := strconv.Atoi(fmt.Sprint(v)); err == nil {
			return i
		}
	}
	return def
}

func anyToInt64WithDefault(value any, def int64) int64 {
	if value == nil {
		return def
	}

	switch v := value.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	case string:
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i
		}
	default:
		if i, err := strconv.ParseInt(fmt.Sprint(v), 10, 64); err == nil {
			return i
		}
	}
	return def
}

func anyToBoolWithDefault(value any, def bool) bool {
	if value == nil {
		return def
	}

	switch v := value.(type) {
	case bool:
		return v
	case string:
		if b, err := parseBool(v); err == nil {
			return b
		}
	case int, int64, float64:
		// Non-zero numbers are true
		if anyToIntWithDefault(v, 0) != 0 {
			return true
		}
		return false
	default:
		if b, err := parseBool(fmt.Sprint(v)); err == nil {
			return b
		}
	}
	return def
}

func anyToFloat64WithDefault(value any, def float64) float64 {
	if value == nil {
		return def
	}

	switch v := value.(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	default:
		if f, err := strconv.ParseFloat(fmt.Sprint(v), 64); err == nil {
			return f
		}
	}
	return def
}

func parseBool(str string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(str)) {
	case "true", "t", "yes", "y", "1", "on", "enabled":
		return true, nil
	case "false", "f", "no", "n", "0", "off", "disabled", "":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean string: %s", str)
	}
}
