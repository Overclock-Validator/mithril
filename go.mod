module github.com/Overclock-Validator/mithril

go 1.24.0

toolchain go1.24.2

replace github.com/gagliardetto/solana-go => github.com/palmerlao/solana-go v0.0.0-20250609100755-01c2412e5333

replace github.com/gagliardetto/binary => github.com/palmerlao/binary v0.0.0-20250617062159-3054b4d33aed

require (
	github.com/VividCortex/ewma v1.2.0
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/charmbracelet/bubbles v0.21.0
	github.com/charmbracelet/bubbletea v1.3.10
	github.com/charmbracelet/lipgloss v1.1.0
	github.com/cockroachdb/pebble v1.1.5
	github.com/gagliardetto/binary v0.8.0
	github.com/gagliardetto/solana-go v1.12.1-0.20250116180958-3786857b86e5
	github.com/google/nftables v0.3.0
	github.com/linxGnu/grocksdb v1.8.0
	github.com/mattn/go-isatty v0.0.20
	github.com/minio/sha256-simd v1.0.1
	github.com/mr-tron/base58 v1.2.0
	github.com/novifinancial/serde-reflection/serde-generate/runtime/golang v0.0.0-20220519162058-e5cd3c3b3f3a
	github.com/prometheus/client_golang v1.20.4
	github.com/segmentio/textio v1.2.0
	github.com/spf13/cobra v1.9.1
	github.com/stretchr/testify v1.11.1
	github.com/twmb/murmur3 v1.1.8
	github.com/vbauerster/mpb/v8 v8.4.0
	golang.org/x/sync v0.17.0
	golang.org/x/sys v0.39.0
	gopkg.in/natefinch/lumberjack.v2 v2.2.1
	gopkg.in/yaml.v3 v3.0.1
	k8s.io/klog/v2 v2.100.1
)

require (
	github.com/aymanbagabas/go-osc52/v2 v2.0.1 // indirect
	github.com/charmbracelet/colorprofile v0.2.3-0.20250311203215-f60798e515dc // indirect
	github.com/charmbracelet/x/ansi v0.10.1 // indirect
	github.com/charmbracelet/x/cellbuf v0.0.13-0.20250311204145-2c3ea96c31dd // indirect
	github.com/charmbracelet/x/term v0.2.1 // indirect
	github.com/cockroachdb/errors v1.11.3 // indirect
	github.com/cockroachdb/fifo v0.0.0-20240606204812-0bbfbd93a7ce // indirect
	github.com/cockroachdb/logtags v0.0.0-20230118201751-21c54148d20b // indirect
	github.com/cockroachdb/redact v1.1.5 // indirect
	github.com/cockroachdb/tokenbucket v0.0.0-20230807174530-cc333fc44b06 // indirect
	github.com/erikgeiser/coninput v0.0.0-20211004153227-1c3628e74d0f // indirect
	github.com/getsentry/sentry-go v0.27.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/snappy v0.0.5-0.20220116011046-fa5810519dcb // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/lucasb-eyer/go-colorful v1.2.0 // indirect
	github.com/mattn/go-localereader v0.0.1 // indirect
	github.com/muesli/ansi v0.0.0-20230316100256-276c6243b2f6 // indirect
	github.com/muesli/cancelreader v0.2.2 // indirect
	github.com/muesli/termenv v0.16.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/onsi/gomega v1.27.6 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/common v0.67.4 // indirect
	github.com/rogpeppe/go-internal v1.12.0 // indirect
	github.com/xo/terminfo v0.0.0-20220910002029-abceb7e1c41e // indirect
	github.com/zeebo/assert v1.3.0 // indirect
	go.yaml.in/yaml/v2 v2.4.3 // indirect
)

require (
	github.com/DataDog/zstd v1.5.7
	github.com/Overclock-Validator/crypto v0.0.0-20250307094320-aaf52fac5261 // indirect
	github.com/Overclock-Validator/gnark-crypto v0.0.0-20250309203346-2a67ed08a105
	github.com/bits-and-blooms/bitset v1.22.0 // indirect
	github.com/consensys/bavard v0.1.31-0.20250406004941-2db259e4b582 // indirect
	github.com/dchest/blake2b v1.0.0 // indirect
	github.com/decred/dcrd/crypto/blake256 v1.0.1 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.0.1 // indirect
	github.com/dolthub/maphash v0.1.0 // indirect
	github.com/fsnotify/fsnotify v1.7.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/gorilla/websocket v1.5.0 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/holiman/uint256 v1.3.2 // indirect
	github.com/ipfs/go-log/v2 v2.5.1 // indirect
	github.com/magiconair/properties v1.8.7 // indirect
	github.com/mimoo/StrobeGo v0.0.0-20181016162300-f8f6d4d2b643 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/mmcloughlin/addchain v0.4.0 // indirect
	github.com/pelletier/go-toml/v2 v2.2.2 // indirect
	github.com/prometheus/client_model v0.6.2 // Indirect dependency
	github.com/rpcpool/yellowstone-grpc/examples/golang v0.0.0-20250409203454-bb3a44a2f723
	github.com/sagikazarmark/locafero v0.4.0 // indirect
	github.com/sagikazarmark/slog-shim v0.1.0 // indirect
	github.com/smcio/go-ethereum v1.15.12-0.20250621134551-8b739407bb93
	github.com/sourcegraph/conc v0.3.0 // indirect
	github.com/spf13/afero v1.11.0 // indirect
	github.com/spf13/cast v1.6.0 // indirect
	github.com/spf13/viper v1.19.0
	github.com/subosito/gotenv v1.6.0 // indirect
	go.opencensus.io v0.24.0 // indirect
	golang.org/x/text v0.30.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250115164207-1a7da9e5054f // indirect
	google.golang.org/grpc v1.71.1
	gopkg.in/ini.v1 v1.67.0 // indirect
	rsc.io/tmplfunc v0.0.3 // indirect
)

require (
	filippo.io/edwards25519 v1.0.0
	github.com/Overclock-Validator/bgls v0.0.0-20250309141600-b7db1bfbf3fa
	github.com/Overclock-Validator/solana-snapshot-finder-go v0.0.0-20251229023828-edd283e1db59
	github.com/Overclock-Validator/wide v0.0.0-20250221123529-f80959d02044
	github.com/acarl005/stripansi v0.0.0-20180116102854-5a71ef0e047d // indirect
	github.com/andres-erbsen/clock v0.0.0-20160526145045-9e14626cd129 // indirect
	github.com/benbjohnson/clock v1.3.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/blendle/zapdriver v1.3.1 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dgryski/go-sip13 v0.0.0-20200911182023-62edffca9245
	github.com/ethereum/go-ethereum v1.15.12-0.20250620111820-f26b5653e8bf
	github.com/fatih/color v1.16.0 // indirect
	github.com/filecoin-project/go-jsonrpc v0.8.0
	github.com/gagliardetto/treeout v0.1.4 // indirect
	github.com/gammazero/deque v1.0.0
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/golang/protobuf v1.5.4
	github.com/google/go-cmp v0.7.0
	github.com/gtank/merlin v0.1.1
	github.com/gtank/ristretto255 v0.1.2
	github.com/iden3/go-iden3-crypto v0.0.16
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.18.0
	github.com/klauspost/cpuid/v2 v2.2.4 // indirect
	github.com/logrusorgru/aurora v2.0.3+incompatible // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/maypok86/otter v1.2.4
	github.com/mdlayher/netlink v1.7.3-0.20250113171957-fbb4dce95f42 // indirect
	github.com/mdlayher/socket v0.5.0 // indirect
	github.com/mitchellh/go-testing-interface v1.14.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mostynb/zstdpool-freelist v0.0.0-20201229113212-927304c0c3b1 // indirect
	github.com/nixberg/chacha-rng-go v0.1.0
	github.com/oasisprotocol/curve25519-voi v0.0.0-20230904125328-1f23a7beb09a
	github.com/panjf2000/ants/v2 v2.10.0
	github.com/pierrec/lz4/v4 v4.1.22
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/samber/lo v1.49.1
	github.com/spf13/pflag v1.0.6 // indirect
	github.com/streamingfast/logging v0.0.0-20230608130331-f22c91403091 // indirect
	github.com/tidwall/btree v1.7.0
	github.com/zeebo/blake3 v0.2.3
	go.mongodb.org/mongo-driver v1.12.2 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	go.uber.org/ratelimit v0.2.0 // indirect
	go.uber.org/zap v1.24.0 // indirect
	golang.org/x/crypto v0.43.0
	golang.org/x/exp v0.0.0-20240904232852-e7e105dedf7e
	golang.org/x/net v0.46.0 // indirect
	golang.org/x/term v0.38.0
	golang.org/x/time v0.9.0
	google.golang.org/protobuf v1.36.10
)
