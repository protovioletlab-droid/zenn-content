---
title: "同じ自宅ラボをself-host OTelとSplunk Observability Cloudの両方で観る — 非対称二枚看板の運用記"
emoji: "🌉"
type: "tech"
topics: ["opentelemetry", "splunk", "observability", "prometheus", "homelab"]
published: true
---

## この記事について

自宅ラボでAIエージェント(Claude Code / Codex CLI)を常時運用し、その観測基盤をOpenTelemetry + Prometheus + Tempoで自前運用しています。1ヶ月で実事件を4つ捕まえた話は[別記事](https://zenn.dev/chemy_pvl/articles/otel-homelab-agent-incident-log)に書きました。

本記事はその続編で、**同じテレメトリをSplunk Observability Cloudのフリーアカウントにも流し、self-hostと商用SaaSの2枚看板で運用してみた**記録です。書くのは次の内容です。

1. Prometheus→SignalFxブリッジの設計と実装(標準ライブラリのみ・常駐なし・全コード方針つき)
2. 送る指標を3層に育てた過程と、MTS(メトリクス時系列)を浪費しない次元設計
3. cgroup v2から固定unitのメモリを直接採取する話(と`memory.events.local`の誤帰属事故)
4. ダッシュボードとdetectorをAPIで冪等プロビジョニングする話(SignalFlowの実物つき)
5. 「送れたはず」で終わらせないSignalFlow読み戻し検収
6. フリーアカウントの現実的な限界と、self-host/SaaSの役割分担の結論

例によって、登場する数値・コード・事件はすべて実環境の実物です。うまくいかなかった設計判断も、時系列を誤魔化さずそのまま書きます。ホスト名・unit名・jobラベルのみ公開用に仮名化しています。

## 非対称に持つ、という構成

まず全体像です。この構成は意図的に非対称です。

- **self-host(OTel + Prometheus + Tempo)**: 無料・恒久・全系列を持つ一次事件簿エンジン。約2万系列を15日retentionで保持
- **Splunk O11y Cloud(フリーアカウント)**: 選抜した数十系列だけを送るオーバーレイ。SaaSのUI・検知器を本物の事件で評価する

![非対称二枚看板の全体構成](/images/otel-contest/fig-b1-asymmetric.png)

「どちらが良いか比較する」のではなく、最初から役割を分けています。理由は2つ。

1つ目は現実的な話で、フリーアカウントに全系列を流すのはMTS的にも筋が悪く、逆にself-host側を捨てて全面SaaS化すると、枠や規約の変更が観測の連続性を人質に取ります。**恒久の正は自分の手元、SaaSは選抜オーバーレイ**なら、どちらが消えても致命傷になりません。

2つ目は評価の話です。商用SaaSの検討は、営業デモやサンプルデータでやるより、**本物の事件を流して「自分の環境の問題がこのUIでどう見えるか」を確かめる**のが一番早い。self-host側で事件簿が既に回っているので、同じテレメトリをSaaSにも見せれば、比較は自動的に「実事件ベース」になります。

## ブリッジの設計4原則

エージェントやexporterの計装は一切変えず、**Prometheusを唯一の取り出し口**にして、5分毎のcronがSignalFx ingest APIへgaugeをPOSTします。Python標準ライブラリのみ(urllib + json)・常駐なし・実測ピークRSS 30MB未満。監視スタック本体には一切書き込まない読み取り専用の設計です。

コードより先に、決めたルールを4つ書きます。実装はこの4つの帰結です。

**1. 指標名と次元は一度決めたら折らない。** SaaS側に溜まった系列(MTS)は指標名+次元の組で識別されます。名前を変えると別系列になり、過去比較が全部死にます。self-host側は自分でrelabelできますが、SaaS側の蓄積は取り返しがつきません。改名したくなったら「追加」し、旧名の送信は重複期間を置いてから止める。

**2. counterは `increase(5m)` をgaugeで送る。** ブリッジは5分毎の単発実行なので、counterの生値を送るとSaaS側でrate計算の面倒(リセット検出・実行間隔の揺れ)を抱えます。Prometheus側で `increase(5m)` に畳んでgauge単一型で送ると、SaaS側は「5分毎の増分」をそのまま読むだけになり、ブリッジも送信APIもgauge 1種類で済みます。型を1つに絞ると、送信部は本当に単純になります。

**3. 動的な次元(PID・session ID・一時ホスト名)は送らない。** MTS数はフリーアカウントの実質的な上限です。動的次元を1つ入れると、MTSは「その次元が取り得た値の数」だけ増殖します。ホスト名と役割ラベル(固定unit名など)だけに絞れば、MTS総数は設計時に数えられる固定値になります。

**4. tokenが未配置ならskipして正常終了(exit 0)。** 秘密ファイルの不在は障害ではなく「未接続」という状態です。ここをエラーにすると、tokenを配る前の環境でcronが毎晩エラー通知を撃ちます。逆にPrometheusに到達できない場合は明確な異常なのでexit 1で通知に乗せる。**「何が正常な欠損で、何が異常か」を終了コードで区別する**のは、無人cronの基本です。

![ブリッジ設計4原則のまとめ](/images/otel-contest/fig-b3-principles.png)

## 実装ウォークスルー

送る指標はPromQLのホワイトリストで定義します(実物から抜粋)。

```python
JOB_SELECTOR = 'job=~"pve-general|ai-core-node|agent-host-node"'

QUERIES = {
    "homelab.node.up": f"up{{{JOB_SELECTOR}}}",
    "homelab.node.cpu.utilization": (
        f"100 - (avg by (instance) "
        f"(rate(node_cpu_seconds_total{{mode=\"idle\",{JOB_SELECTOR}}}[5m])) * 100)"),
    "homelab.node.memory.used_percent": (
        f"100 * (1 - (node_memory_MemAvailable_bytes{{{JOB_SELECTOR}}} "
        f"/ node_memory_MemTotal_bytes{{{JOB_SELECTOR}}}))"),
    "homelab.node.swap.used_bytes": (
        f"node_memory_SwapTotal_bytes{{{JOB_SELECTOR}}} "
        f"- node_memory_SwapFree_bytes{{{JOB_SELECTOR}}}"),
    "homelab.node.memory.psi_waiting_percent_5m": (
        f"100 * rate(node_pressure_memory_waiting_seconds_total{{{JOB_SELECTOR}}}[5m])"),
    "homelab.node.memory.major_faults_5m": (
        f"increase(node_vmstat_pgmajfault{{{JOB_SELECTOR}}}[5m])"),
    "homelab.node.memory.oom_kills_5m": (
        f"increase(node_vmstat_oom_kill{{{JOB_SELECTOR}}}[5m])"),
}
```

Prometheusの `instance` ラベル(IP:port)は、そのままSaaSの次元にせず短縮ホスト名へ写像します。

```python
INSTANCE_TO_HOST = {
    "<pve01のinstance>": "pve01",
    "<agent-hostのinstance>": "agent-host",
    # 休眠中ノードも残す: 系列が無ければ何も送らないだけ
}
```

この写像表は「送ってよいホストのホワイトリスト」も兼ねていて、**表に無いinstanceの行は黙って捨てます**。Prometheusにscrape対象を足した時に、意図せずSaaS送信まで増えるのを防ぐ二重ゲートです。休眠中のノードは表に残したまま、系列が来なければ何も送らない。**欠損を0で埋めない・素通しする**のはブリッジ全体の一貫した方針です(0は「観測できて0だった」という立派なデータであり、欠損の代役にしてはいけません)。

ここで過去の設計ミスを1つ白状します。旧構成のブリッジは、Prometheusにscrapeされていなかったホスト1台だけ「ローカル採取」の特別扱いで拾っていました。移設後、そのホストがPrometheusのscrape対象に入ったのにローカル採取を残していたら、**同じホストが `host=旧名` と `host=新名` の2つの次元で二重報告**されるところでした。移設時にローカル採取のコードごと削除して解決。「取り出し口はPrometheusだけ」という原則に例外を作ると、例外は必ず後で二重計上として牙を剥きます。

送信部はgauge一括POSTのみで、dry-runを最初から作りました。

```bash
$ python3 splunk_o11y_bridge.py --dry-run
# → 送信予定のdatapoint全件をJSONで表示し、送信はしない
# splunk-o11y-bridge: dry-run datapoints=40 token=present
```

cron投入前の検収はこのdry-runで「点数と次元」を目視し、実送信1回でHTTP成功を見て、次のcron発火で同じ点数が出ることまで確認して完了としました。現在は1回の実行で40 datapointを送っています。内訳はnode基本4指標×対象ホスト+深掘り5指標+オーバーレイ9指標+固定3unit×cgroup4指標で、**設計表の上で足し算できる固定値**です。dry-runの点数が設計表とズレたら、それ自体が「どこかのクエリが欠損している」の検知になります。

送信ペイロードは単純で、gauge配列を1回POSTするだけです。

```json
{"gauge": [
  {"metric": "homelab.node.memory.psi_waiting_percent_5m",
   "value": 0.0,
   "dimensions": {"host": "agent-host", "source": "prometheus-bridge"}}
]}
```

全datapointに `source=prometheus-bridge`(cgroup直接採取分は `source=cgroup-local`)という出所次元を付けています。SaaS側で系列を見た時に「これはどの経路から来たか」が次元だけで分かり、将来OTelネイティブ送信など別経路を足しても衝突しません。**出所を次元に刻む**のは、経路が2枚以上ある構成での安い保険です。

接続先はrealm(リージョン)をファイルから読んで `https://ingest.<realm>.signalfx.com/v2/datapoint` を組み立てます。当初はrealmをコードに直書きしていましたが、設定ファイル化しました。realmはアカウント作成時に決まる値で、間違えると認証エラーではなく「届かない」系の分かりにくい失敗になります。

cron運用の細部も書いておきます。5分毎の単発実行で、多重起動はflockで防止(前回が刺さっていたら次はスキップ)。実測ピークRSSは30MB未満なので、systemdのリソース制御をかけるまでもない軽さですが、この「軽さの実測値」自体を導入時に一度取って宣言しておくと、ホストのメモリ設計(前記事のOOM序列の話)に組み込めます。ログはstdoutに1行サマリ(sent/skip/点数)、異常はstderr+exit 1でcronのメール/通知経路に乗せる。無人で5分毎に走るものは、正常時の出力を1行に抑えないとログが観測を邪魔し始めます。

### increase(5m)方式の注意点

counterを5分増分のgaugeに畳む方式は単純さが取り柄ですが、正直に限界も書きます。cronの発火間隔とPromQLの `[5m]` 窓は厳密には同期しないので、**増分の取りこぼしや二重計上が境界で起こり得ます**。またcounterがリセット(プロセス再起動)を跨ぐと、`increase` はリセットを補正しますが完全ではありません。うちの用途は「事件の有無と規模感を掴む」観測なので、この誤差は許容しています。会計級の正確さが要る値(課金カウンタ等)をこの方式で送ってはいけません。その場合はOTLPネイティブのcumulative counterとして送る設計に変えるべきで、「gauge単一型の単純さ」とのトレードオフです。

## 送る指標は3層に育てた

最初から全部設計できたわけではなく、必要が生まれた順に3層になりました。

**第1層(基本): nodeの4指標。** `up` / CPU使用率 / メモリ使用率 / ディスク使用率をホスト毎に。まずSaaS側に「地面」を作ります。この時点ではdatapointは1回十数点です。

**第2層(比較用オーバーレイ): 9指標。** self-host側の事件簿が捕まえた事件(GPU沈黙・移設の継ぎ目・パイプライン異常)を、Splunk側でも同じテレメトリで観直せるようにする層です(実物から抜粋)。

```python
OVERLAY_QUERIES = {
    # GPU: 電源OFF運用ホストは平常時欠損 = 送らない
    "homelab.gpu.utilization": ("gpu_utilization_percent", "host"),
    "homelab.gpu.temperature_celsius": ("gpu_temperature_celsius", "host"),
    # AIエージェント稼働: host単位に畳む。セッション等の細粒度は送らない
    "homelab.ai.claude.tokens_5m": (
        "sum by (host_name) (increase(claude_code_token_usage_tokens_total[5m]))",
        "host_name"),
    "homelab.ai.claude.active_seconds_5m": (
        "sum by (host_name) (increase(claude_code_active_time_seconds_total[5m]))",
        "host_name"),
    # collector健全性: 受入停止・詰まりの比較用
    "homelab.otelcol.refused_metric_points_5m": (
        "sum(increase(otelcol_receiver_refused_metric_points[5m]))", None),
    "homelab.otelcol.exporter_queue_size": ("sum(otelcol_exporter_queue_size)", None),
}
```

推したいのは最後の2つ、**OTel collector自身の健全性指標を商用側にも送っておく**ことです。SaaS側でグラフが止まった時、「self-host側が沈黙したのか、ブリッジが死んだのか、SaaSの取り込みが詰まったのか」の切り分けを、SaaS側の画面だけで一歩進められます。観測経路が2枚あると、経路自体の相互監視ができるのは地味に便利です。

**第3層(深掘り): メモリ圧の解剖セット。** これが本記事の本題で、次の2節で書きます。

## 動機: 7月のメモリ圧事件

self-host側の7月最大の事件はswap枯渇級のメモリ圧でした(詳細は記事A)。実測で:

- エージェント実行環境のブート以降ピークRSS 71.6GiB、バースト時はswap 8GiBを使い切り
- memory PSI(some, avg60)ピーク **86.82%**、swap失敗カウンタの増分約2.26億
- 原因unitはCodexのアプリサーバ(ピーク25.7GiB、swap 7.96GiB)と特定

この事件はself-host側のunit別cgroup計測で解剖できましたが、当時のSplunk側には第1層の「ホストメモリ使用率」しかなく、**SaaS側だけを見ていたら「ホストが苦しい」以上のことは何も言えなかった**はずです。

そこで「host使用率で終わらせず、原因unitまでSaaS側で追える」状態を作ることにしました。ここは正直に書きますが、以降の深掘り層と検知器は**事件の後に**整備したものです。事件をSplunkがリアルタイムに捕まえた、という話ではありません。次の同種事件を、今度はSplunk側でも一発で解剖できるようにするための備えです。

追加したのは、ホスト毎の深掘り5指標:

```
memory.available_bytes        # 「空き」ではなく「実際に使える」量
swap.used_bytes               # swapの絶対量(使用率より早く立ち上がる)
memory.psi_waiting_percent_5m # PSI: 実際にメモリ待ちで止まった時間割合
memory.major_faults_5m        # スラッシングの直接証拠
memory.oom_kills_5m           # 最後の審判
```

そして原因特定用に、**固定した3つのagent unitだけ**のcgroup計測です。

## cgroup v2から直接採取する

unit別メモリはPromQL経由ではなく、ブリッジがcgroup v2のファイルを直接読みます(採取対象ホスト上でcronが動いているため)。実物の骨格はこうです。

```python
AGENT_UNITS = ("codex-app-server", "claude-rc-1", "claude-rc-2")
CGROUP_METRICS = {
    "memory.current": "homelab.agent.memory.current_bytes",
    "memory.peak":    "homelab.agent.memory.peak_bytes",
}
CGROUP_EVENT_METRICS = {
    "high":     "homelab.agent.memory.high_events_total",
    "oom_kill": "homelab.agent.memory.oom_kills_total",
}

for unit in AGENT_UNITS:
    unit_dir = CGROUP_ROOT / f"{unit}.service"
    # memory.current / memory.peak: 読めなければ送らない(0で埋めない)
    # イベントは memory.events.local から読む(events は配下を含む)
    events = _read_memory_events(unit_dir / "memory.events.local")
```

設計点は3つあります。

**unitは固定リスト。** 「system.slice配下を全部列挙」すれば網羅できますが、unit名が次元になるのでMTSが読めなくなり、一時的なscopeやテンプレートunitがゴミ系列を量産します。メモリを大きく使い得る犯人候補(Codexアプリサーバと2系統のエージェント常駐)を3つ決め打ちし、**新規MTSは固定で十数本**に抑えました。候補が増えたらリストを直してデプロイする。この不便さは、MTSが常に数えられることの対価として受け入れています。

**`memory.peak` を現在値と別に送る。** 5分毎のサンプリングでは、瞬間ピークは現在値の系列から抜け落ちます。cgroup v2の `memory.peak` はカーネルが保持してくれる高水位標なので、これを送っておくと「サンプリングの谷間で何GiBまで行ったか」が失われません。

**イベントは `memory.events.local` から読む。** cgroup v2の `memory.events` は配下サブツリーのイベントを含むため、親から読むと子のMemoryHigh/OOMを親のせいに誤帰属します。実際、self-host側の整備初期にこの誤帰属をやらかしました(記事A参照)。ブリッジは最初から `.local` を読み、「そのunit自身に何が起きたか」だけを送ります。

停止中のunitはcgroupディレクトリごと消えますが、これも欠損素通しの原則通り「読めなければ送らない」。SaaS側では系列が途切れることが「unitが居ない」の表現になります。

## ダッシュボードとdetectorはコードで作る

Splunk側の資産はUIで組まず、APIで冪等プロビジョニングするスクリプトにしました。引数なしで実行するとplan(何を作るか)だけをJSONで表示し、`--apply` で作成。**同名資産があれば作らず既存を返す**ので、再実行しても増殖しません。IaCツールを持ち込むほどの規模ではないけれど手作業には戻りたくない、という中間解です。

チャートはSignalFlow(Splunkのストリーム計算言語)のプログラムとして定義します(実物から抜粋)。

```python
line_chart(
    "Memory PSI waiting percent",
    "処理がメモリ待ちで止まった時間割合。使用率より先に体感劣化を示す。",
    "A = data('homelab.node.memory.psi_waiting_percent_5m', "
    "filter=filter('host', 'agent-host')).publish(label='PSI')"),
line_chart(
    "Agent unit current memory",
    "固定3unitの現在値。動的PIDは次元にしない。",
    "A = data('homelab.agent.memory.current_bytes', "
    "filter=filter('host', 'agent-host')).publish(label='current')",
    "Binary"),
```

ダッシュボードは8チャート構成にしました。実物のチャート名と意図を並べます。

| チャート | 答える質問 |
|---|---|
| Host memory used percent | ホストは苦しいか(入口。これ単体では原因は分からない) |
| Available memory and swap | availableの低下とswap増加が同時に起きているか |
| Memory PSI waiting percent | 「使用率が高い」ではなく「実際に待たされている」か |
| Major faults and OOM kills | スラッシングの物証と、最悪事象の有無 |
| Agent unit current memory | いま誰が食っているか(固定3unit) |
| Agent unit boot peak memory | サンプリングの谷間で誰が何GiBまで行ったか |
| Agent MemoryHigh and OOM events | cgroup上限に誰が何回当たったか(累積の増分が事件) |
| Claude activity | その時間帯、エージェントは何をしていたか(sessions/active/tokens) |

前半4枚が「ホストに何が起きたか」、次の3枚が「誰のせいか」、最後の1枚が「その時エージェントは何をしていたか」。メモリ事件の調査で人間が実際に問う順番に、上から並べた形です。特に最後の1枚は入れてよかったチャートで、メモリの山とエージェント稼働の山が同じ画面で重なると、「バーストはエージェントの仕事量由来か、リークか」の当たりが最初の10秒でつきます。

実物のダッシュボードはこうなっています(実UIのスクリーンショット。表示名は本文同様に仮名化):

![Splunk実画面: Agent Memory Pressureダッシュボード(8チャート)](/images/otel-contest/shot-splunk-dashboard.png)

detectorは3ルールで、これもSignalFlowの実物を載せます。

```
psi  = data('homelab.node.memory.psi_waiting_percent_5m', ...).publish(label='psi');
swap = data('homelab.node.swap.used_bytes', ...).publish(label='swap');
oom  = data('homelab.node.memory.oom_kills_5m', ...).publish(label='oom');
detect(when(psi > 10, lasting='5m')).publish('memory PSI high');
detect(when(swap > 1073741824, lasting='10m')).publish('swap over 1GiB');
detect(when(oom > 0)).publish('OOM kill detected')
```

- memory PSI > 10% が5分継続 → **Warning**
- swap > 1GiB が10分継続 → **Warning**
- 5分内のOOM kill > 0 → **Critical**

作成されたdetectorのUIはこの形です(3ルールと現在の発火数0が見える):

![Splunk実画面: detector 3ルール](/images/otel-contest/shot-splunk-detector.png)

severityの割り方は「前哨2枚をWarning、確定事象1枚をCritical」です。PSIとswapは事件の予兆なので観察を促す強さに留め、OOM killは既に被害が出た事実なので最強で鳴らす。閾値は全部、7月事件の実測から逆算しています。平常時のPSIは0%なので10%は「明確に異常だが手遅れではない」水準。swapは8GiB枯渇の事件に対して1GiBで前哨を張る(`lasting='10m'` で一時的なスパイクは無視)。OOM killは1件でもCritical。**実事件の実測が先にあると、閾値の議論が「なんとなく80%」から卒業できます。**

その「当て込み」を可視化したのが次の図です。7月事件の実測(self-host側)に、後から決めたdetector閾値を重ねています。7/22のバーストは両閾値を大きく突き抜けて確実に鳴る一方、7/20にあるswapの持続的な微増(約0.6GiB)は閾値未満で鳴らない。この「鳴らない側の確認」が閾値設計の後半戦です。

![detector閾値と7月事件の実測](/images/otel-contest/fig-b2-detector-thresholds.png)

APIまわりで実務的な点を2つ。作成前に `/detector/validate` へ同じbodyを投げて検証が通ることを確かめてから本作成すること。そして冪等性は「名前で検索して、あれば作らない」という素朴な実装で十分成立すること(検索APIの結果から完全一致名を選ぶだけ)。IDをローカルにstateとして持つ方式より、SaaS側を正とするこの方式の方が、手元のstateが消えても壊れません。

## 検収: SignalFlow読み戻しまでやる

プロビジョニングと送信の検収は、次の段階を踏みました。

1. dry-runでdatapointの点数と次元を目視(送信なし)
2. 実送信1回でHTTP成功を確認、次のcron発火でも同点数を確認
3. planでダッシュボード・detectorの作成予定を確認 → apply
4. **再applyで同じIDが返り、資産が増殖しないことを確認**
5. **SignalFlow APIで読み戻す**: `homelab.agent.memory.current_bytes` を1時間分クエリし、3 unitぶんのMTSとdataイベントが実際に返ることを確認

強調したいのは5です。ingest APIのHTTP成功は「届いた」の証明であって、「クエリ可能な形で溜まった」の証明ではありません。次元の付け間違い・型の不一致・rollupの誤解は、読み戻して初めて分かります。送信側のログがどれだけ緑でも、**消費側のAPIで読めるまでは完了扱いにしない**。これはSaaSに限らず、自前のPrometheusでも同じ規律です(記事Aの「着地するまでが検収」と同じ話です)。

もう1つ、認証情報は役割で分離しました。ingest token(送信専用)とAPI token(資産作成・クエリ用)は別物として管理し、ブリッジにはingest tokenしか渡しません。送信係が漏れても資産は壊せない、という最小権限の素朴な適用です。どちらのtokenも、スクリプトは値を一切出力しません。

## 第2層はSplunk側でどう見えているか

深掘り層の話が長くなったので、第2層(GPU・AI稼働・collector健全性)の使われ方も短く報告します。

GPU指標は、self-host側の事件簿にある「電源off運用ホストの系列は電源投入中だけ生える」という性質がSplunk側でもそのまま見えます。系列が無い期間を異常と映すか平常と映すかはダッシュボード側の解釈の問題で、うちは「生えている期間だけ見る」チャートにしています。detectorをGPU沈黙に張らないのも同じ理由です(沈黙の3値分類はself-host側の夜間チェックが担当し、SaaS側は重複させない)。**同じ検知を2枚に張ると、事件のたびに2回対応することになります。** 検知の担当は経路ごとに決めておくのが実用的でした。

AI稼働(tokens_5m / active_seconds_5m / sessions_started_5m)は、単体のアラート対象ではなく**文脈指標**として効いています。メモリ・GPUのどのチャートと重ねても「その時エージェントが働いていたか」を添えられるのは、AIエージェント環境の観測ならではの便利さです。ここをhost単位に畳んでセッション粒度を捨てたのは前述のMTS設計の帰結ですが、1ヶ月使って粒度不足で困った場面はありませんでした。細粒度が要る調査は、どのみち全ラベルを持つself-host側でやるからです。

## 通知先をあえて繋がない

detectorの通知先は現在**ゼロ**です。意図的にそうしています。

作りたての検知器を通知に直結すると、初期の偽陽性がそのまま人間への割り込みになります。記事Aで書いた通り、self-host側でも「trace欠落アラートの偽陽性を3回解消してから検知器を直す」round tripがありました。新しい検知器はまず**SaaS内のアラート画面で検知実績と偽陽性率を観察**し、信頼できると分かってから通知経路(メール等)を開く。アラートの信頼はごく簡単に毀損し、回復には時間がかかります。「検知の開始」と「通知の開始」を分けるだけで、この失敗は避けられます。

## フリーアカウントの現実

2026年7月時点で確認した範囲では、Splunk Observability CloudのFree Editionは期限なし・15ホスト・機能制限は緩め、という位置づけです(規約と公式案内は、自分が使い始める時点の原文を必ず確認してください)。

始める時の実務手順は次の通りでした。

1. アカウント作成時に**realm(リージョン)を控える**。以後のingest/API両エンドポイントのホスト名に入ります
2. tokenを**用途別に2枚**発行する。ingest token(送信専用)とAPI token(資産作成・SignalFlowクエリ用)。ブリッジには前者だけを配る
3. 送信を始める前にMTSの設計表(指標×次元の掛け算)を作り、総数を数えておく
4. 送信開始後、Usage Analyticsで実消費が設計表と一致するかを答え合わせする

実運用で意識したのは2点です。

**MTSの消費が実質の上限。** だからこそ動的次元の禁止・固定unit方式・counterの5分増分化が効いてきます。そして実際の消費量は憶測せず、Splunk自身が提供する組織メトリクス(`sf.org.numCustomMetrics` / `sf.org.limit.customMetricTimeSeries`)をUsage Analyticsで確認する運用にしました。**上限との距離も、また観測対象**です。設計段階で「新規MTSは十数本」と数えてあるので、この確認は答え合わせとして機能します。

**ホスト数のカウント。** 15ホスト枠に対し、送るホストは役割で選抜しています(全ノード送信は最初からしない)。ブリッジのホワイトリスト(前述の写像表)がそのまま枠の管理台帳になっています。

この範囲でも、unit別メモリの深掘りダッシュボードと3ルールのdetectorまで普通に作れました。ホームラボの観測オーバーレイとしては十分実用です。

## 比較して見えたこと

1ヶ月併用して、それぞれの強みは想像よりはっきり分かれました。

**Splunk側が良かったところ。** ダッシュボードとdetectorが同じSignalFlowで書けるので、「グラフで見ている式」と「検知している式」が乖離しません。self-host側ではGrafanaのパネルとアラートルールを別々に育てて式がズレる事故が起こりがちです。またdetectorの `lasting` のような継続時間条件が言語組み込みで、素朴に書けます。

**self-host側が良かったところ。** 全系列・全ラベルが手元にあるので、事件調査で「あの系列も見たい」に即応できます。SaaS側は選抜した数十本しか無いので、深掘りの終着点は結局self-host側でした。retentionも保全も自分の裁量です。

**両方に共通の学び。** MTSを数えながら指標を選抜する作業は、実は「この環境で本当に見るべき指標は何か」を強制的に考えさせる良い制約でした。制約なしのself-host側では2万系列を無自覚に抱えていましたが、SaaS側に送る数十本を選ぶ過程で、監視の一軍が明文化されました。node_exporterが吐く数百系列のうち、事件の調査で実際に開いたのはavailable・swap・PSI・major fault・OOM killと、cgroupのcurrent/peak/イベントでほぼ全部です。選抜リストは「もし明日、監視を数十系列しか持てないとしたら何を残すか」という問いへの、実事件で検証済みの答えになっています。

もう1つ、二重化の副産物として**観測経路の相互監視**が働き始めました。self-host側のcollectorやPrometheusが止まれば、Splunk側では第2層のcollector健全性指標とnode系列が同時に途切れて見えます。逆にブリッジやSaaS取り込みが止まっても、self-host側は無傷です。単一の観測スタックは「観測が死んだこと」を自分では言えませんが、非対称の2枚看板は、お互いの死をもう片方が観測できます。狙って設計した機能ではないのですが、運用してみると安心感への寄与が大きい性質でした。

| 役割 | self-host (OTel+Prometheus) | Splunk O11y Cloud (free) |
|---|---|---|
| 全系列の一次保存 | ◎ 恒久・無制限 | — 選抜のみ |
| 事件の第一検知 | ◎ 夜間チェック+アラート | ○ detector(PSI/swap/OOM) |
| 深掘りUI | ○ Grafana | ◎ ダッシュボード+detector統合 |
| 式の一元性 | △ パネルとアラートが別管理 | ◎ SignalFlowで共通 |
| コスト | 電気代のみ | 無料枠内 |
| 消える条件 | 自分が壊した時 | 規約・枠の変更 |

## まとめ

**恒久の正はself-hostに置き、SaaSは選抜テレメトリのオーバーレイとして使う。** この非対称なら、SaaS側の枠や規約が変わっても観測の連続性は失われず、SaaSの良いUIと検知器は本物の事件で評価できます。

そしてこの自由を担保しているのがOpenTelemetryと、Prometheus互換という共通の取り出し口です。エージェントの計装は1ヶ所(OTLP)へ送るだけ。その先を何枚に分けるか、どのSaaSを試すか、いつやめるかは、全部あとから決められる。ベンダーを選ばない計装を先に敷いておくことが、SaaSを気軽に試せる条件そのものでした。

最後に、この構成の「やめ方」も決めてあります。ブリッジはcron 1行を止めれば送信が止まり、self-host側には何の影響もありません。SaaS側の資産はプロビジョニングスクリプトが正なので、別のSaaSに乗り換えるならスクリプトの移植だけで再現できます。**始める前に、影響なく止められることを確認しておく**のは、SaaS併用を気軽に試すためのもう1つの条件だと思います。

次の同種事件が来たら、今度はSplunkのdetectorが先に鳴るのか、self-hostの夜間チェックが先に捕まえるのか。2枚看板の競争を楽しみにしています。

