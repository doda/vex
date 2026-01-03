package fts

func testConfigNoStemming() *Config {
	cfg := DefaultConfig()
	cfg.Stemming = false
	return cfg
}
