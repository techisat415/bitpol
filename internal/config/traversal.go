// Package config contains process-wide traversal defaults.
package config

const DefaultMaxHops = 4

func MaxHops(override int) int {
	if override > 0 {
		return override
	}
	return DefaultMaxHops
}
