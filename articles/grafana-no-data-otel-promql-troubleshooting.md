---
title: "GrafanaのNo dataを受信カウンタから切り分ける：OTelで受信済みでも見えない4原因"
emoji: "📊"
type: "tech"
topics: [homelab, opentelemetry, grafana, prometheus, observability]
published: true
---

Grafanaのパネルが「No data」で埋まると、監視基盤ごと止まったように見えます。ですが、可視化が空であることと、テレメトリの配管が止まったことは別の事象です。この記事は、自宅ラボの監視スタックで「No data」に遭遇したときに、赤い画面ではなく受信カウンタから経路を切り分けた記録の要約です。

## 何が起きたか

そのとき、OpenTelemetry Collectorが受信したメトリクスは40,020点、spanは20件でした。failedとrefusedはいずれも0です。Prometheusのターゲットは2系統ともupで、GPUノード2台の使用率は58%と98%、最終サンプルは約15秒前でした。Tempoも別途21件のspanを受信しています。

CollectorのacceptedとTempoのreceivedは、それぞれ別コンポーネントが持つ独立した累積カウンタです。起点やリセット時点が一致する保証はないため、20と21という値の差だけで矛盾とは判断しません。ここでは「各層で受信が起きている証拠」として読みます。

それでもGrafanaの収集ヘルス表示は「No data」でした。調べると、保存済みダッシュボードは0件でした。データソースは設定され、配管も動いていたのに、見るためのパネル集合が保存されていなかったのです。「No data」は診断名ではなく症状名だ、という前提から始めます。

## 先に見るのは受信カウンタ

障害なのか表示だけの問題なのかを短時間で判断したいとき、最初の問いは「Grafanaが赤いか」ではありません。送信元からCollectorまで届いているか、です。

受信の生死は、たとえば次のPromQLで確認します。

```promql
rate(otelcol_receiver_accepted_metric_points[5m])
```

拒否の有無は次の形です。

```promql
rate(otelcol_receiver_refused_metric_points[5m])
```

acceptedが増えており、refusedやfailedが増えていなければ、「そもそも送っていない」「Collectorが受け取れずに捨てている」という候補は大きく後退します。今回もこの時点で、配管そのものは健全と判断できました。

「No data」の候補は、おおむね次の順で潰していきます。

1. **そもそも送っていない** — acceptedの増加を見る。増えなければ送信元からCollectorまでを疑う。
2. **拒否・失敗している** — refused / failedの増加を見る。増えるなら受信設定や送信形式を疑う。
3. **クエリ名が違う** — Prometheusに実在するメトリクス名と照合する。
4. **ダッシュボードが無い** — 期待するパネル集合が保存・配備されているか確認する。

受信カウンタ → メトリクス名 → ダッシュボードという順序です。先にパネル設定を何時間も眺めるより、accepted / refused / failedを見た方が速く絞れます。

## `_total`を補わない

もう一つの原因は、PromQLが存在しない名前を参照していたことでした。この環境のCollectorでは、受信カウンタは `otelcol_receiver_accepted_metric_points` でした。`_total` が付く前提でクエリを組むと、存在しない系列を参照することがあります。たとえば `otelcol_exporter_send_failed_metric_points_total` のような名前を当然のように使うと、配管が正常でも「No data」になります。

`_total` の有無はCollectorのバージョンや公開スキーマに依存します。名前を見慣れた形式から補完せず、Prometheus側で現在存在するメトリクス名を列挙して照合するのが安全です。

## 正常時に「No data」を出さない

失敗数や拒否数は、無いことが正常な系列です。しかしPrometheusでは、値が0の系列が常に存在するとは限りません。素のクエリでは空の結果になり、Statパネルは「No data」と表示します。障害なしを「データなし」と見せないため、optionalな系列は `or vector(0)` で包みます。

```promql
((sum(increase(otelcol_receiver_failed_metric_points[24h]))) or vector(0))
```

複数のoptional系列を合算する場合も、各項を先に0で包みます。

```promql
( A or vector(0) ) + ( B or vector(0) ) + ( C or vector(0) )
```

受信が動いていてもアプリ由来のサンプルが古い場合は別問題です。鮮度は `time() - timestamp(app_metric)` の一般形で確認し、運用に合った閾値で監視します。

## ダッシュボードはコードから冪等に配る

今回、データソースは設定済みでしたが、保存済みダッシュボードは0件でした。修理後は、Overview、GPU、Collection Health、OTLP Pipeline Healthの4種をコード化して配備し、同じUIDへの `overwrite: true` で冪等に更新しました。旧UIDも同じJSONで上書きし、古いブックマークから陳腐なパネルが開かれないようにしています。

再配備後は、すべてのパネルのPromQLターゲットがデータを返しました。手動チェックでは、総系列数7,116、24時間のAI関連系列72、トレース検索ヒット1、エクスポータキューは0/1000でした。

メトリクスがまだ1系統だけで、まず送受信そのものを学びたい段階なら、ダッシュボード整備を急ぐ必要はありません。まずはaccepted / refused / failedを確実に読める状態で十分です。ただし、手作業で作ったパネルに重要な判断を任せるようになった時点で、再生成できる形への移行を検討します。

## 教訓・チェックリスト

- ダッシュボードをコードから同じUIDで再生成できるか。
- 失敗数・拒否数のようなoptional系列を、正常時に0として表示できるか。
- パネルのメトリクス名を、現在のPrometheusのスキーマといつ照合したか。
- 別コンポーネントの累積カウンタ(Collectorとバックエンド)は、値の差だけで矛盾と決めつけず、各層の受信証拠として読む。

「No data」は少なくとも四つの原因に分かれます。送っていない、Collectorに拒否・失敗された、メトリクス名が違う、配管は正常でもダッシュボードが保存されていない。画面が再び赤くなっても、最初に見る場所は受信カウンタです。症状に名前を付けて終わらせず、経路を順番に確認すれば、必要以上に不安を増やさずに済みます。

## 詳細ログ

切り分けの実際の手順と、修復に使ったダッシュボード配備の詳細は、元記事にまとめています。

https://www.proto-violet-lab.com/grafana-no-data-otel-promql-troubleshooting/?utm_source=zenn&utm_medium=referral&utm_campaign=observability-implementation_e2_crosspost
