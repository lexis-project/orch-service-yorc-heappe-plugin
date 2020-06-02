module github.com/lexis-project/yorc-heappe-plugin/v1

require (
	github.com/bramvdbogaerde/go-scp v0.0.0-20191005185035-c96fe084709e
	github.com/hashicorp/consul v1.2.3
	github.com/pkg/errors v0.9.1
	github.com/ystia/yorc/v4 v4.0.0
	golang.org/x/crypto v0.0.0-20200510223506-06a226fb4e37
	gopkg.in/cookieo9/resources-go.v2 v2.0.0-20150225115733-d27c04069d0d
)

// Due to this capital letter thing we have troubles and we have to replace it explicitly
replace github.com/Sirupsen/logrus => github.com/sirupsen/logrus v1.4.1

go 1.13
