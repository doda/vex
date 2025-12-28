package guardrails

import "github.com/vexsearch/vex/internal/config"

// FromConfig creates a guardrails Config from a config.GuardrailsConfig.
func FromConfig(cfg config.GuardrailsConfig) Config {
	return Config{
		MaxNamespaces:            cfg.GetMaxNamespaces(),
		MaxTailBytesPerNamespace: cfg.MaxTailBytesPerNamespace(),
		MaxConcurrentColdFills:   cfg.GetMaxConcurrentColdFills(),
	}
}
