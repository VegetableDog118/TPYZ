module github.com/pingcap-incubator/tinykv

require (
	github.com/BurntSushi/toml v0.4.1
	github.com/Connor1996/badger v1.5.1-0.20210202034640-5ff470f827f8
	github.com/DataDog/zstd v1.4.8 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f
	github.com/dgraph-io/ristretto v0.1.0 // indirect
	github.com/dgryski/go-farm v0.0.0-20200201041132-a6ae2369ad13 // indirect
	github.com/docker/go-units v0.4.0
	github.com/fsnotify/fsnotify v1.5.1 // indirect
	github.com/gogo/protobuf v1.3.2
	github.com/golang/glog v1.0.0 // indirect
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/btree v1.0.1
	github.com/juju/errors v0.0.0-20210818161939-5560c4c073ff
	github.com/juju/loggo v0.0.0-20210728185423-eebad3a902c4 // indirect
	github.com/juju/mgo/v2 v2.0.0-20210414025616-e854c672032f // indirect
	github.com/juju/retry v0.0.0-20210818141810-5526f6f6ff07 // indirect
	github.com/juju/testing v0.0.0-20210324180055-18c50b0c2098 // indirect
	github.com/juju/utils/v2 v2.0.0-20210305225158-eedbe7b6b3e2 // indirect
	github.com/juju/version v0.0.0-20210303051006-2015802527a8 // indirect
	github.com/klauspost/cpuid v1.3.1 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/ncw/directio v1.0.5 // indirect
	github.com/ngaut/log v0.0.0-20210830112240-0124ec040aeb // indirect
	github.com/onsi/ginkgo v1.16.4 // indirect
	github.com/opentracing/opentracing-go v1.2.0
	github.com/petar/GoLLRB v0.0.0-20210522233825-ae3b015fd3e9
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errcode v0.3.0
	github.com/pingcap/errors v0.11.5-0.20190809092503-95897b64e011
	github.com/pingcap/log v0.0.0-20210906054005-afc726e70354
	github.com/pingcap/tidb v1.1.0-beta.0.20200309111804-d8264d47f760
	github.com/pingcap/tipb v0.0.0-20210830034902-3d2699ad59b9
	github.com/pkg/errors v0.9.1
	github.com/prometheus/common v0.30.0 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20200410134404-eec4a21b6bb0 // indirect
	github.com/rogpeppe/go-internal v1.8.0 // indirect
	github.com/shirou/gopsutil v3.21.8+incompatible
	github.com/sirupsen/logrus v1.8.1
	github.com/stretchr/testify v1.7.0
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.uber.org/zap v1.19.1
	golang.org/x/crypto v0.0.0-20210915214749-c084706c2272 // indirect
	golang.org/x/mod v0.5.0 // indirect
	golang.org/x/net v0.0.0-20210916014120-12bc252f5db8
	golang.org/x/sys v0.0.0-20210915083310-ed5796bab164 // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/time v0.0.0-20210723032227-1f47c861a9ac // indirect
	google.golang.org/genproto v0.0.0-20210916144049-3192f974c780 // indirect
	google.golang.org/grpc v1.40.0
	google.golang.org/protobuf v1.27.1 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
	honnef.co/go/tools v0.2.1 // indirect
//github.com/shirou/gopsutil v2.20.8+incompatible
)

go 1.13

replace github.com/pingcap/tidb => github.com/pingcap-incubator/tinysql v0.0.0-20200518090433-a7d00f9e6aa7

replace google.golang.org/grpc => google.golang.org/grpc v1.26.0
