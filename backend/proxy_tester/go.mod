module proxy_tester

go 1.25.5

require github.com/unbound-future-admin/backend/pkg/proxypool v0.0.0

require gopkg.in/yaml.v3 v3.0.1 // indirect

replace github.com/unbound-future-admin/backend/pkg/proxypool => ../pkg/proxypool
