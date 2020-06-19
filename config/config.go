package config

type Config struct {
	Driver string
	DNS    string

	MaxConns     int
	MaxIdleConns int
	MaxLifetime  int
}

type PolicyOptions struct {
	Mode   string
	Params interface{}
}

type ClusterConfig struct {
	Driver string
	Master *Config
	Slaves []*Config
	Policy PolicyOptions
}

type DBConfig map[string]interface{}
