#!/usr/bin/env python3
"""Build and publish a versioned daily warehouse from a verified frozen source batch."""
import argparse
from datetime import date
import json
from pathlib import Path
import re
import shutil
import sys
from time import perf_counter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import CONTRACT, PLATFORM, TABLES, exclusive_lock, publish, safe_name, sha256, utc_now, validate_snapshot, write_json

MODELS = dict(re.findall(r'-- model: (\w+)\s*\n(.*?);', (PLATFORM / 'sql/models.sql').read_text(encoding='utf-8'), re.S))
OUTPUTS = {
    'dim_shop_snapshot': ('dim', None), 'dim_voucher_snapshot': ('dim', None), 'dim_date': ('dim', None),
    'dwd_order_created': ('dwd', 'biz_date'), 'dwd_order_snapshot': ('dwd', None), 'dwd_order_event': ('dwd', 'event_date'),
    'dws_shop_order_day': ('dws', 'biz_date'), 'ads_shop_order_rank': ('ads', 'biz_date'),
    'ads_order_status': ('ads', None), 'ads_voucher_performance': ('ads', 'biz_date'),
}


def scalar(spark, sql):
    return spark.sql(sql).first()[0]


def payment_time_quality(spark):
    """Whole-snapshot gate: tolerate small legacy drift, never change business-day attribution."""
    row = spark.sql("""SELECT
        COUNT(CASE WHEN pay_time_utc IS NULL THEN 1 END) AS missing,
        COUNT(CASE WHEN skew_ms>=1000 AND skew_ms<60000 AND pay_date=event_date THEN 1 END) AS warning,
        COUNT(CASE WHEN skew_ms>=60000 OR pay_date<>event_date THEN 1 END) AS blocking,
        MAX(skew_ms) AS max_skew_ms
        FROM (SELECT s.pay_time_utc,e.event_date,
            to_date(from_utc_timestamp(s.pay_time_utc,'Asia/Shanghai')) AS pay_date,
            abs(unix_millis(s.pay_time_utc)-unix_millis(e.occurred_at_utc)) AS skew_ms
            FROM dwd_order_event e JOIN dwd_order_snapshot s ON e.order_id=s.order_id
            WHERE e.event_type='ORDER_PAID')""").first().asDict()
    return row


def build(args, stage, manifest, source_counts):
    from pyspark import StorageLevel
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as f
    from pyspark.sql.types import StringType, StructField, StructType
    spark = (SparkSession.builder.appName('hmdp-offline-' + args.run_id)
        .config('spark.sql.session.timeZone', 'UTC').config('spark.sql.ansi.enabled', 'true')
        .config('spark.sql.shuffle.partitions', '2').config('spark.ui.enabled', 'false').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    checks, warnings, counts = {}, [], {}
    timing, started = {}, perf_counter()
    try:
        (stage / 'ods').mkdir(parents=True, exist_ok=True)
        shutil.copyfile(args.input_root / 'manifest.json', stage / 'ods/source_manifest.json')
        for table in TABLES:
            source = args.input_root / (table + '.jsonl')
            ods = stage / 'ods' / table
            ods.mkdir(parents=True)
            shutil.copyfile(source, ods / source.name)
            schema = StructType([StructField(column, StringType(), True) for column in CONTRACT['source_tables'][table]])
            df = spark.read.schema(schema).option('mode', 'FAILFAST').json(str(ods / source.name))
            df.createOrReplaceTempView(table)
            checks[table + '_source_count'] = df.count() == source_counts[table]
        for name, sql in MODELS.items():
            df = spark.sql(sql)
            if args.cache_policy == 'memory-and-disk' and name in {
                    'dwd_order_created', 'dwd_order_snapshot', 'dwd_order_event', 'dws_shop_order_day'}:
                df = df.persist(StorageLevel.MEMORY_AND_DISK)
            df.createOrReplaceTempView(name)
        timing['source_and_model_setup_seconds'] = perf_counter() - started
        phase = perf_counter()
        checks['order_primary_key'] = scalar(spark, 'SELECT COUNT(*)=COUNT(DISTINCT order_id) FROM dwd_order_created')
        checks['event_primary_key'] = scalar(spark, 'SELECT COUNT(*)=COUNT(DISTINCT event_id) FROM dwd_order_event')
        checks['snapshot_primary_key'] = scalar(spark, 'SELECT COUNT(*)=COUNT(DISTINCT order_id) FROM dwd_order_snapshot')
        checks['orders_required'] = scalar(spark, "SELECT COUNT(*) FROM dwd_order_created WHERE order_id IS NULL OR user_id IS NULL OR voucher_id IS NULL OR biz_date IS NULL") == 0
        checks['event_dates'] = scalar(spark, 'SELECT COUNT(*) FROM dwd_order_event WHERE event_date IS NULL OR occurred_at_utc IS NULL OR processed_at_utc IS NULL') == 0
        checks['event_foreign_key'] = scalar(spark, 'SELECT COUNT(*) FROM dwd_order_event e LEFT ANTI JOIN dwd_order_created o ON e.order_id=o.order_id') == 0
        checks['paid_event_unique_order'] = scalar(spark, "SELECT COUNT(*)=COUNT(DISTINCT order_id) FROM dwd_order_event WHERE event_type='ORDER_PAID'")
        paid_time = payment_time_quality(spark)
        checks['paid_time_present'] = paid_time['missing'] == 0
        checks['paid_time_consistent'] = paid_time['blocking'] == 0
        if paid_time['warning']:
            warnings.append({'rule': 'paid_time_skew', 'rows': paid_time['warning'],
                             'warning_ms': 1000, 'blocking_ms': 60000})
        checks['dim_keys'] = all(scalar(spark, f'SELECT COUNT(*)=COUNT(DISTINCT {key}) FROM {model}') for model, key in [('dim_shop_snapshot', 'shop_id'), ('dim_voucher_snapshot', 'voucher_id')])
        missing_shop = scalar(spark, 'SELECT COUNT(*) FROM dwd_order_created o LEFT ANTI JOIN dim_shop_snapshot s ON o.shop_id=s.shop_id')
        missing_voucher = scalar(spark, 'SELECT COUNT(*) FROM dwd_order_created o LEFT ANTI JOIN dim_voucher_snapshot v ON o.voucher_id=v.voucher_id')
        pending = scalar(spark, "SELECT COUNT(*) FROM tb_business_outbox b LEFT ANTI JOIN tb_order_event_history h ON b.id=h.event_id WHERE b.kind IN ('ORDER_CREATED','ORDER_PAID','ORDER_CLOSED')")
        if pending:
            warnings.append({'rule': 'unprojected_order_events', 'rows': pending})
        if missing_shop or missing_voucher:
            warnings.append({'rule': 'missing_dimensions', 'shop_rows': missing_shop, 'voucher_rows': missing_voucher})
        unknown = scalar(spark, 'SELECT COUNT(*) FROM dwd_order_created WHERE NOT amount_known')
        if unknown:
            warnings.append({'rule': 'unknown_amount', 'rows': unknown})
        checks['dws_unique_key'] = scalar(spark, "SELECT COUNT(*)=COUNT(DISTINCT struct(biz_date,shop_id)) FROM dws_shop_order_day")
        # Full snapshot reconciliation before selecting one reporting date.
        for metric, reference in [
            ('created_orders', 'SELECT COUNT(*) FROM dwd_order_created'),
            ('created_known_cents', 'SELECT SUM(pay_cents) FROM dwd_order_created'),
            ('paid_orders', "SELECT COUNT(*) FROM dwd_order_event WHERE event_type='ORDER_PAID'"),
            ('closed_events', "SELECT COUNT(*) FROM dwd_order_event WHERE event_type='ORDER_CLOSED'"),
            ('paid_known_cents', "SELECT SUM(o.pay_cents) FROM dwd_order_event e JOIN dwd_order_created o ON e.order_id=o.order_id WHERE e.event_type='ORDER_PAID'")]:
            checks['reconcile_' + metric] = (scalar(spark, f'SELECT SUM({metric}) FROM dws_shop_order_day') or 0) == (scalar(spark, reference) or 0)
        quality = {'passed': all(checks.values()), 'checks': checks, 'warnings': warnings,
                   'source_counts': source_counts, 'payment_time': paid_time,
                   'unprojected_order_events': pending, 'missing_dimensions': {'shops': missing_shop, 'vouchers': missing_voucher}}
        write_json(stage / 'quality.json', quality)
        if not quality['passed']:
            raise ValueError('Quality gate failed: ' + str([name for name, ok in checks.items() if not ok]))
        timing['quality_seconds'] = perf_counter() - phase
        phase = perf_counter()
        as_of = manifest.get('snapshot_as_of', manifest.get('scenario_as_of'))
        for name, (layer, partition) in OUTPUTS.items():
            df = spark.table(name)
            if layer in ('dws', 'ads') and partition == 'biz_date':
                df = df.filter(f.col('biz_date') == f.lit(args.biz_date).cast('date'))
            df = (df.withColumn('_source_batch', f.lit(args.source_batch))
                .withColumn('_source_env', f.lit(manifest['source_env']))
                .withColumn('_snapshot_as_of', f.lit(as_of)))
            counts[name] = df.count()
            writer = df.coalesce(1).write.mode('errorifexists')
            if partition:
                writer = writer.partitionBy(partition)
            writer.parquet(str(stage / layer / name))
        timing['output_seconds'] = perf_counter() - phase
        timing['spark_work_seconds'] = perf_counter() - started
        timing['cache_policy'] = args.cache_policy
        timing['spark_conf'] = {key: spark.conf.get(key) for key in [
            'spark.sql.shuffle.partitions', 'spark.sql.adaptive.enabled', 'spark.sql.autoBroadcastJoinThreshold']}
        timing['master'] = spark.sparkContext.master
        timing['driver_memory'] = spark.sparkContext.getConf().get('spark.driver.memory')
        write_json(stage / 'execution.json', timing)
        return quality, counts, spark.version
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-root', type=Path, required=True)
    parser.add_argument('--run-root', type=Path, required=True)
    parser.add_argument('--source-batch', required=True)
    parser.add_argument('--biz-date', required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--fail-before-publish', action='store_true', help='Acceptance fault injection')
    parser.add_argument('--cache-policy', choices=['none', 'memory-and-disk'], default='none')
    args = parser.parse_args()
    safe_name(args.run_id)
    safe_name(args.source_batch)
    if date.fromisoformat(args.biz_date).isoformat() != args.biz_date:
        raise ValueError('biz-date must be YYYY-MM-DD')
    args.input_root, args.run_root = args.input_root.resolve(), args.run_root.resolve()
    manifest, source_counts = validate_snapshot(args.input_root, args.source_batch)
    input_hash = sha256(args.input_root / 'manifest.json')
    final = args.run_root / 'versions' / args.biz_date / args.run_id
    stage = args.run_root / '.staging' / args.biz_date / args.run_id
    if final.exists() or stage.exists():
        raise FileExistsError('Run-id already used; retained versions/staging are immutable, choose a new run-id')
    with exclusive_lock(args.run_root, args.biz_date):
        stage.mkdir(parents=True)
        try:
            quality, counts, spark_version = build(args, stage, manifest, source_counts)
            # Detect changes to the declared frozen source during this run.
            validate_snapshot(args.input_root, args.source_batch)
            if sha256(args.input_root / 'manifest.json') != input_hash:
                raise ValueError('Source manifest changed during run')
            artifacts = {str(p.relative_to(stage)).replace('\\', '/'): sha256(p) for p in stage.rglob('*') if p.is_file()}
            record = {'run_id': args.run_id, 'source_batch': args.source_batch, 'biz_date': args.biz_date,
                'source_env': manifest['source_env'], 'source_manifest_sha256': input_hash,
                'synthetic': manifest.get('synthetic', False),
                'payment_provenance': manifest.get('payment_provenance', manifest.get('payment', 'unknown')),
                'snapshot_as_of': manifest.get('snapshot_as_of', manifest.get('scenario_as_of')),
                'spark_version': spark_version, 'quality': quality, 'model_counts': counts,
                'model_sql_sha256': sha256(PLATFORM / 'sql/models.sql'),
                'artifacts': artifacts, 'completed_at_utc': utc_now()}
            write_json(stage / 'run_manifest.json', record)
            if args.fail_before_publish:
                raise RuntimeError('Injected failure before publication')
            pointer = {'run_id': args.run_id, 'source_batch': args.source_batch, 'biz_date': args.biz_date,
                       'version': final.relative_to(args.run_root).as_posix(), 'manifest_sha256': sha256(stage / 'run_manifest.json')}
            publish(stage, final, args.run_root / 'catalog' / (args.biz_date + '.json'), pointer)
            print(json.dumps({'passed': True, 'published': pointer, 'model_counts': counts, 'warnings': quality['warnings']}, ensure_ascii=False, indent=2))
        except BaseException as failure:
            if stage.exists():
                write_json(stage / 'failure.json', {'run_id': args.run_id, 'failed_at_utc': utc_now(), 'error': str(failure)})
            raise


if __name__ == '__main__':
    main()
