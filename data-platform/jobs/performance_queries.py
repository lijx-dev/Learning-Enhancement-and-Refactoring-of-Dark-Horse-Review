#!/usr/bin/env python3
"""Controlled query pairs over verified DWD Parquet, with plans and SQL metrics."""
import argparse
import hashlib
import json
from pathlib import Path
from statistics import median
import sys
from time import perf_counter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import utc_now, write_json


def logical(rows):
    return sorted(json.dumps(row.asDict(), sort_keys=True, default=str) for row in rows)


def metrics(plan):
    found = []
    iterator = plan.metrics().iterator()
    values = {}
    while iterator.hasNext():
        entry = iterator.next()
        metric = entry._2()
        values[str(entry._1())] = int(metric.value())
    if values:
        found.append({'node': plan.nodeName(), 'values': values})
    children = plan.children().iterator()
    while children.hasNext():
        found.extend(metrics(children.next()))
    return found


def main():
    from pyspark.sql import SparkSession, functions as f
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--version', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--biz-date', required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    spark = (SparkSession.builder.appName('hmdp-sql-experiments')
        .config('spark.sql.session.timeZone', 'UTC').config('spark.sql.ansi.enabled', 'true')
        .config('spark.sql.shuffle.partitions', '4').config('spark.sql.adaptive.enabled', 'false')
        .config('spark.sql.autoBroadcastJoinThreshold', '-1').config('spark.ui.enabled', 'false').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    report = {'started_at_utc': utc_now(), 'spark_version': spark.version, 'pairs': {},
        'query_conf': {'shuffle_partitions': 4, 'adaptive_enabled': False, 'auto_broadcast_threshold': -1},
        'method': 'one warmup per variant then 3 timed actions each, alternating AB/BA/AB; OS caches uncontrolled; collect includes Spark scheduling',
        'notes': 'Pair differences are named explicitly; no distributed throughput or cold-cache claim.'}
    try:
        fact = spark.read.parquet(str(args.version / 'dwd/dwd_order_created'))
        dim = spark.read.parquet(str(args.version / 'dim/dim_shop_snapshot'))
        fact.createOrReplaceTempView('fact')
        dim.createOrReplaceTempView('shops')
        day = args.biz_date
        selected = f"biz_date=DATE '{day}'"
        projection = 'shop_id,COUNT(*) AS orders,SUM(pay_cents) AS cents'
        # Plan evidence only: full and projected scans do different work, so no speed ratio.
        (args.output / 'column-pruning-narrow.txt').write_text(
            fact.select('shop_id', 'pay_cents')._jdf.queryExecution().toString(), encoding='utf-8')
        (args.output / 'column-pruning-wide.txt').write_text(fact._jdf.queryExecution().toString(), encoding='utf-8')
        report['column_pruning'] = {'full_columns': fact.columns, 'query_columns': ['shop_id', 'pay_cents'],
            'evidence': ['column-pruning-narrow.txt', 'column-pruning-wide.txt'], 'timing_comparison': False}

        def pair(name, sql_a, sql_b, expected_nodes=()):
            pair_dir = args.output / name
            pair_dir.mkdir()
            variants = {'baseline': sql_a, 'candidate': sql_b}
            results = {variant: {'sql': sql, 'seconds': [], 'metrics': []} for variant, sql in variants.items()}
            answer = None
            for variant, sql in variants.items():
                rows = logical(spark.sql(sql).collect())
                if answer is None:
                    answer = rows
                elif rows != answer:
                    raise ValueError('Warmup logical mismatch: ' + name)
            for repeat in range(3):
                order = list(variants) if repeat % 2 == 0 else list(reversed(variants))
                for variant in order:
                    df = spark.sql(variants[variant])
                    started = perf_counter()
                    rows = df.collect()
                    elapsed = perf_counter() - started
                    if logical(rows) != answer:
                        raise ValueError('Timed logical mismatch: ' + name)
                    plan = df._jdf.queryExecution()
                    results[variant]['seconds'].append(elapsed)
                    results[variant]['metrics'].append(metrics(plan.executedPlan()))
                    (pair_dir / f'{variant}-{repeat + 1}.txt').write_text(plan.toString(), encoding='utf-8')
            for variant, value in results.items():
                value['median_seconds'] = median(value['seconds'])
                value['plans'] = [f'{name}/{variant}-{i}.txt' for i in range(1, 4)]
            for variant, node in expected_nodes:
                plan = (pair_dir / f'{variant}-1.txt').read_text(encoding='utf-8')
                if node not in plan:
                    raise ValueError(f'Expected {node} missing in {name}/{variant}')
            result = {'results_equal': True, 'result_rows': len(answer),
                'result_sha256': hashlib.sha256('\n'.join(answer).encode()).hexdigest(),
                'variants': results, 'baseline_over_candidate_median': results['baseline']['median_seconds'] / results['candidate']['median_seconds']}
            report['pairs'][name] = result
            write_json(args.output / 'progress.json', report)
            print(json.dumps({'pair': name, 'ratio': result['baseline_over_candidate_median'], 'equal': True}), flush=True)

        pair('partition-pruning',
            f"SELECT {projection} FROM fact WHERE to_date(from_utc_timestamp(created_at_utc,'Asia/Shanghai'))=DATE '{day}' GROUP BY shop_id",
            f'SELECT {projection} FROM fact WHERE {selected} GROUP BY shop_id')
        join = f"FROM fact o JOIN shops d ON o.shop_id=d.shop_id WHERE o.{selected} GROUP BY o.shop_id,d.name"
        pair('broadcast-dimension',
            'SELECT /*+ MERGE(o,d) */ o.shop_id,d.name,COUNT(*) AS orders,SUM(o.pay_cents) AS cents ' + join,
            'SELECT /*+ BROADCAST(d) */ o.shop_id,d.name,COUNT(*) AS orders,SUM(o.pay_cents) AS cents ' + join,
            [('baseline', 'SortMergeJoin'), ('candidate', 'BroadcastHashJoin')])

        layouts = {}
        for label, partitions in [('many-files', 32), ('compact-files', 2)]:
            path = args.output / 'layouts' / label
            started = perf_counter()
            fact.repartition(partitions).write.mode('errorifexists').partitionBy('biz_date').parquet(str(path))
            files = list(path.rglob('*.parquet'))
            selected_files = list((path / ('biz_date=' + day)).glob('*.parquet'))
            layouts[label] = {'write_seconds': perf_counter() - started, 'files': len(files),
                'bytes': sum(p.stat().st_size for p in files), 'selected_day_files': len(selected_files),
                'selected_day_bytes': sum(p.stat().st_size for p in selected_files)}
            spark.read.parquet(str(path)).createOrReplaceTempView(label.replace('-', '_'))
        report['layouts'] = layouts
        pair('small-files', f'SELECT {projection} FROM many_files WHERE {selected} GROUP BY shop_id',
            f'SELECT {projection} FROM compact_files WHERE {selected} GROUP BY shop_id')

        # Show rows placed on each shuffle partition; this is not task runtime.
        distribution = fact.repartition(4, 'shop_id').withColumn('pid', f.spark_partition_id()).groupBy('pid').count().collect()
        report['skew_partition_rows'] = {str(r['pid']): r['count'] for r in distribution}
        report['largest_partition_fraction'] = max(r['count'] for r in distribution) / sum(r['count'] for r in distribution)
        pair('salted-aggregation',
            f'SELECT {projection} FROM fact GROUP BY shop_id',
            'SELECT shop_id,SUM(n) AS orders,SUM(c) AS cents FROM '
            '(SELECT shop_id,pmod(xxhash64(order_id),8) AS salt,COUNT(*) AS n,SUM(pay_cents) AS c '
            'FROM fact GROUP BY shop_id,pmod(xxhash64(order_id),8)) t GROUP BY shop_id')
        report.update(passed=True, completed_at_utc=utc_now())
        write_json(args.output / 'report.json', report)
    except BaseException as failure:
        report.update(passed=False, failure=str(failure))
        write_json(args.output / 'failure.json', report)
        raise
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
