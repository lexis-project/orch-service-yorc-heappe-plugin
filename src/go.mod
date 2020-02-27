module github.com/lexis-project/yorc-heappe-plugin/v1

require (
	github.com/bramvdbogaerde/go-scp v0.0.0-20191005185035-c96fe084709e
	github.com/cheekybits/is v0.0.0-20150225183255-68e9c0620927 // indirect
	github.com/hashicorp/consul v1.2.3
	github.com/pkg/errors v0.8.1
	github.com/xlab/handysort v0.0.0-20150421192137-fb3537ed64a1 // indirect
	github.com/ystia/yorc/v4 v4.0.0-M9
	golang.org/x/crypto v0.0.0-20190308221718-c2843e01d9a2
	gopkg.in/cookieo9/resources-go.v2 v2.0.0-20150225115733-d27c04069d0d
)

// Due to this capital letter thing we have troubles and we have to replace it explicitly
replace github.com/Sirupsen/logrus => github.com/sirupsen/logrus v1.4.1

go 1.13
