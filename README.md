# Pakeloss

Pakelossは、ラボネットワーク向けの分散アクティブ型packet loss測定ツールです。controllerが接続中のagentからfull meshの片方向flowを自動生成し、agent同士がTWAMP Light風のUDP request/response packetを送受信します。

senderとreceiverはrequest sequenceをcontrollerへ報告し、controllerが`tx`、`rx`、`lost = max(tx - rx, 0)`を確定します。response未着はloss判定に使わず、sender側のduplicate/reorder観測にだけ使います。

## 構成

- Control plane: `pakeloss-agent`と`pakeloss-controller`間のgRPC双方向stream
- Measurement plane: agent間のIPv4 UDP request/response
- Operator interface: controller HTTP APIへ接続する`pakelossctl`とTUI
- Storage: controllerのメモリ上のruntime stateと任意のCSV/JSONLログ

controllerはenabledなagentの組み合わせごとに`node-a->node-b`と`node-b->node-a`を生成します。flow設定はcontrollerが所有し、agentごとのsnapshotとして配布します。

## 動作環境と注意事項

- Go 1.26
- GoReleaserの配布対象はLinux amd64
- VRF bindingはLinuxのみ対応
- controllerのTCP 8443（gRPC）とTCP 8080（HTTP）、各agentのUDP listen portが相互到達可能であること

gRPCとHTTPはTLSに対応していません。共有tokenも暗号化されずに送信されるため、信頼できる閉域ネットワーク内でのみ使用してください。サンプルの`change-me-before-use`はデモ用です。実運用前にcontroller、agent、`pakelossctl`で同じ十分に強いtokenへ変更してください。

## ビルドとテスト

```bash
export GOCACHE=/tmp/go-build
export GOMODCACHE=/tmp/go-mod

go fmt ./...
go test ./...
go build ./cmd/pakeloss-controller
go build ./cmd/pakeloss-agent
go build ./cmd/pakelossctl
```

### ローカル負荷試験

Linux環境でcontrollerと3台のagentを起動し、loopback上の6方向flowを1ms間隔で測定します。実行には`bash`、`curl`、`jq`が必要です。

```bash
./scripts/local-load-test.sh
```

既定の実行時間は15秒です。環境変数で変更できます。

```bash
LOAD_TEST_DURATION_SECONDS=60 ./scripts/local-load-test.sh
```

各flowの送受信packet数が実行秒数あたり500以上で、全agentとflowが稼働していれば成功です。GitHub Actionsではpull requestごとに15秒、毎週月曜のJST 03:00頃と手動実行時に60秒の負荷試験を行います。失敗時を含むプロセスログはworkflow artifactへ保存されます。

`v*`形式のtagをpushするとGitHub Actionsのrelease workflowが起動します。3バイナリ、README、LICENSE、サンプル設定を含むLinux amd64用archiveとchecksumを生成し、GitHub Releaseへ公開します。

## ローカルデモ

最初にcontrollerを起動します。

```bash
go run ./cmd/pakeloss-controller --config configs/pakeloss-controller.toml
```

別々のterminalで3台のagentを起動します。

```bash
go run ./cmd/pakeloss-agent --config configs/pakeloss-agent-a.toml
go run ./cmd/pakeloss-agent --config configs/pakeloss-agent-b.toml
go run ./cmd/pakeloss-agent --config configs/pakeloss-agent-c.toml
```

状態確認とTUI起動:

```bash
go run ./cmd/pakelossctl --config configs/pakeloss-controller.toml agents
go run ./cmd/pakelossctl --config configs/pakeloss-controller.toml flows
go run ./cmd/pakelossctl --config configs/pakeloss-controller.toml tui
```

flowの初期状態は`stopped`です。TUIで`s`を押すと全flowの測定を開始します。

設定ファイルを使わない場合は必要な値をflagで指定できます。tokenはcontroller、agent、`pakelossctl`のすべてで必須です。

```bash
go run ./cmd/pakeloss-controller \
  --grpc-addr 127.0.0.1:8443 \
  --http-addr 127.0.0.1:8080 \
  --token change-me-before-use

go run ./cmd/pakeloss-agent \
  --agent-id node-a \
  --controller-addr 127.0.0.1:8443 \
  --token change-me-before-use \
  --udp-listen 0.0.0.0:40001 \
  --udp-advertise 192.0.2.10:40001

go run ./cmd/pakelossctl \
  --api 127.0.0.1:8080 \
  --token change-me-before-use flows
```

## TUI

- `q`: 終了
- `r`: 再読み込み
- `a` / `f` / `m`: agent / flow / directional matrix view
- `l`: lossがあるflowだけを表示
- `o`: Flow、Act、Outage、Lostのsort順を切り替え
- `e`: 選択中agentのenable/disable
- `s`: 全flowのstart/stop
- `p`: 選択中flowのpause/resume
- `c`: 全flowをrestart

flow viewは`Outage`、`LossTime`、`Lost`、`Reorder`、`Dup`、`TX`、`RX`とloss graphを表示します。graphは1文字を1秒として、画面幅に応じて最大240秒を表示します。matrix viewは行を送信元、列を宛先として片方向flowの累積`Outage`を表示します。

## 設定

controllerは接続してきたagentを自動登録します。enabledなagentのunordered pairごとに2本のdirectional flowを作り、無効化されたagentをmeshから除外します。agentのenable/disableは全flowが`stopped`のときだけ可能です。agentが切断してもruntime stateには残り、statusが`offline`になります。

主なcontroller設定:

- `[server]`: gRPC/HTTP listen address。既定値は`127.0.0.1:8443`と`127.0.0.1:8080`
- `[auth].token`: 必須の共有token
- `[measurement].report_finalize_delay`: sender/receiver reportの確定待ち。既定値`2s`
- `[measurement].report_bucket_factor`: reportにまとめるprobe数。既定値`10`
- `[measurement].outage_threshold_ms`: 連続欠損を通信断とする閾値。既定値`100`
- `[flow_defaults].interval_ms`: probe間隔。既定値`10`
- `[flow_defaults].packet_size`: IPv4 headerとUDP headerを含むpacket全体のbyte数。サンプル値`96`
- `[flow_defaults].source_port_count`: 論理flowでround-robinするUDP source port数。既定値`8`
- `[flow_defaults].loss_confirm_window_ms`: response未着の補助観測待ち時間。既定値`2000`

report windowは`interval_ms × report_bucket_factor`です。サンプル設定では100msになります。controllerはreportをsequence単位で照合し、API/TUI用に直近1秒の値と60秒・240秒の履歴を計算します。

agentの`advertise_addr`にはpeerから到達可能なUDP addressを指定します。省略時は具体的な`listen_addr`を使用します。wildcard listenの場合、non-loopback IPv4が1つだけなら自動選択し、複数候補がある場合はエラーになります。

`on_controller_disconnect`の既定値は`continue`です。既存flowを維持して1秒ごとに再接続します。`stop`では切断時に全flowを停止します。

## HTTP API

すべてのendpointで`Authorization: Bearer <token>`が必要です。

- `GET /api/v1/status`
- `GET /api/v1/agents`
- `GET /api/v1/flows`
- `POST /api/v1/flows/start|stop|restart`
- `POST /api/v1/flows/{id}/pause|resume`
- `POST /api/v1/agents/{id}/enable|disable`

flow responseはstate、packet/report設定、1秒値と累積counter、loss ratio、isolated loss、outage、測定不能時間、60秒・240秒のloss履歴を返します。履歴は常にそれぞれ60要素と240要素です。

## 結果ログ

`[result_log]`の各pathは任意です。省略した出力形式は無効になります。

- `csv` / `jsonl`: flowごとの1秒集計結果
- `summary_csv` / `summary_jsonl`: セッション終了時のflow集計
- `debug_jsonl`: sender/receiver sequence照合の診断情報
- `outage_event_csv` / `outage_event_jsonl`: 通信断の開始・終了イベント

1本以上のflowが`running`になるとセッションを開始し、最後のflowが停止すると終了します。通常・診断・outageログはtimestampとsession ID付きのファイルへ出力され、summaryは指定ファイルへ追記されます。`flush_interval`は書き出し周期で、既定値は`10s`です。

- `tx`: senderが送信したrequest数
- `rx`: receiverが観測したrequest数
- `lost`: controllerがsequence照合で確定した欠落数
- `loss_time_ms`: `lost × interval_ms`
- `outage_ms`: 閾値以上連続した完全欠損時間
- unmeasurable: sender report自体を得られずlossを確定できない時間

CSV timestampは`[result_log].timezone`（既定値`Asia/Tokyo`）へ変換した`YYYY-MM-DD HH:MM:SS`形式です。JSONL timestampはUTCのRFC 3339形式です。outage eventは開始時に`started`として記録され、復旧または測定終了時に同じrecordが`ended`へ更新されます。

## 既知の制限

- full TWAMP-Controlおよび外部TWAMP機器とのwire互換はありません。
- `proto/control.proto`は意図したschemaで、実装はprotoc生成コードではなくgRPC JSON codecを使用します。
- IPv6、TLS/mTLS、HA、長期DB、Web UI、Prometheus連携には対応していません。
- duplicate/reorderはreflected responseの受信順による補助情報です。

## License

[MIT License](LICENSE)
