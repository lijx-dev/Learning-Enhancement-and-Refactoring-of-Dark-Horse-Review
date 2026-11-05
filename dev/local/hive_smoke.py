#!/usr/bin/env python3
"""Isolated embedded Hive catalog: partition repair, snapshot switch and reopen."""
import argparse
from datetime import date
import json
import os
from pathlib import Path

MODELS = {'orders': ('dwd_order_created', 'biz_date'), 'events': ('dwd_order_event', 'event_date')}


def literal(value):
    if "'" in value or '\\' in value:
        raise ValueError('Invalid SQL path/date literal')
    return "'" + value + "'"


def main():
    from pyspark.sql import SparkSession
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--version', type=Path, required=True)
    parser.add_argument('--previous-version', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--biz-date', required=True)
    parser.add_argument('--mode', choices=['build', 'reopen'], required=True)
    args = parser.parse_args()
    day = date.fromisoformat(args.biz_date).isoformat()
    output = args.output.resolve()
    if args.mode == 'build':
        output.mkdir(parents=True, exist_ok=False)
    elif not (output / 'metastore_db').is_dir():
        raise ValueError('No existing catalog to reopen')
    os.chdir(output)
    spark = (SparkSession.builder.appName('hmdp-hive-compatible-' + args.mode)
        .config('spark.sql.warehouse.dir', (output / 'warehouse').as_uri())
        .config('spark.hadoop.javax.jdo.option.ConnectionURL',
                'jdbc:derby:;databaseName=' + (output / 'metastore_db').as_posix() + ';create=true')
        .config('spark.sql.session.timeZone', 'UTC').config('spark.sql.ansi.enabled', 'true')
        .config('spark.sql.shuffle.partitions', '2').config('spark.ui.enabled', 'false')
        .enableHiveSupport().getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    report = {'passed': False, 'mode': args.mode, 'spark_version': spark.version,
        'catalog_implementation': spark.conf.get('spark.sql.catalogImplementation'),
        'biz_date': day, 'snapshots': {}, 'checks': {}}
    try:
        def location(version, model):
            path = version.resolve() / 'dwd' / model
            if not path.is_dir():
                raise ValueError('DWD missing: ' + str(path))
            return path

        def query():
            rows = lambda sql: [r.asDict() for r in spark.sql(sql).collect()]
            return {
                'created_orders': spark.table('orders').count(), 'all_events': spark.table('events').count(),
                'created': rows('SELECT CAST(biz_date AS STRING) AS biz_date,shop_id,COUNT(*) AS orders,'
                    'COUNT(DISTINCT user_id) AS users,SUM(pay_cents) AS known_cents,'
                    'SUM(CASE WHEN amount_known THEN 0 ELSE 1 END) AS unknown_amount,'
                    'SUM(CASE WHEN is_free THEN 1 ELSE 0 END) AS free_orders '
                    'FROM orders GROUP BY biz_date,shop_id ORDER BY biz_date,shop_id'),
                'paid': rows("SELECT CAST(e.event_date AS STRING) AS biz_date,o.shop_id,COUNT(*) AS orders,"
                    "SUM(o.pay_cents) AS known_cents FROM events e JOIN orders o ON e.order_id=o.order_id "
                    "WHERE e.event_type='ORDER_PAID' GROUP BY e.event_date,o.shop_id ORDER BY e.event_date,o.shop_id"),
                'closed': rows("SELECT CAST(event_date AS STRING) AS biz_date,COUNT(*) AS orders "
                    "FROM events WHERE event_type='ORDER_CLOSED' GROUP BY event_date ORDER BY event_date"),
                'partitions': {table: sorted(r[0] for r in spark.sql('SHOW PARTITIONS ' + table).collect()) for table in MODELS}}

        if args.mode == 'build':
            spark.sql('CREATE DATABASE hmdp_lab')
        spark.sql('USE hmdp_lab')
        if args.mode == 'build':
            for table, (model, partition) in MODELS.items():
                path = location(args.previous_version, model)
                schema = spark.read.parquet(str(path)).schema
                columns = ','.join('`' + field.name + '` ' + field.dataType.simpleString()
                    for field in schema if field.name != partition)
                spark.sql(f'CREATE EXTERNAL TABLE {table} ({columns}) PARTITIONED BY ({partition} DATE) '
                    'STORED AS PARQUET LOCATION ' + literal(path.as_posix()))
                spark.sql('MSCK REPAIR TABLE ' + table)
            report['snapshots']['before'] = query()
            existing = spark.sql('SHOW PARTITIONS events').first()[0].split('=', 1)[1]
            files = sorted(location(args.previous_version, MODELS['events'][0]).rglob('*.parquet'))
            spark.sql('ALTER TABLE events DROP PARTITION (event_date=' + literal(existing) + ')')
            report['checks']['external_partition_files_preserved'] = all(p.is_file() for p in files)
            missing = [r[0] for r in spark.sql('SHOW PARTITIONS events').collect()]
            report['checks']['partition_removed_from_catalog'] = 'event_date=' + existing not in missing
            spark.sql('MSCK REPAIR TABLE events')
            report['checks']['repair_restored_results'] = query() == report['snapshots']['before']
            # Root LOCATION alone does not update existing partition locations.
            # This isolated catalog is switched serially; no multi-table atomicity claim.
            for table, (model, partition) in MODELS.items():
                path = location(args.version, model)
                spark.sql('ALTER TABLE ' + table + ' SET LOCATION ' + literal(path.as_posix()))
                for row in spark.sql('SHOW PARTITIONS ' + table).collect():
                    value = row[0].split('=', 1)[1]
                    target = path / (partition + '=' + value)
                    if not target.is_dir():
                        spark.sql(f'ALTER TABLE {table} DROP PARTITION ({partition}={literal(value)})')
                    else:
                        # Path.as_uri() escapes '=' to %3D; Hive SQL would treat that
                        # as a literal directory name. Supply the SQL path directly.
                        spark.sql(f'ALTER TABLE {table} PARTITION ({partition}={literal(value)}) SET LOCATION ' + literal(target.as_posix()))
                spark.sql('MSCK REPAIR TABLE ' + table)
                spark.catalog.refreshTable(table)
        report['snapshots']['after'] = query()
        report['checks']['nonempty_tables'] = all(report['snapshots']['after'][k] > 0 for k in ('created_orders', 'all_events'))
        for table in MODELS:
            detail = {r.col_name: r.data_type for r in spark.sql('DESCRIBE FORMATTED ' + table).collect()}
            report['checks'][table + '_external_hive_serde'] = detail.get('Type') == 'EXTERNAL' and detail.get('Provider') == 'hive'
        plan = spark.sql('EXPLAIN FORMATTED SELECT COUNT(*) FROM events WHERE event_date=DATE ' + literal(day)).first()[0]
        (output / (args.mode + '-explain.txt')).write_text(plan, encoding='utf-8')
        report['checks']['partition_pruning_observed'] = 'PartitionFilters:' in plan and 'event_date' in plan.split('PartitionFilters:')[1].splitlines()[0]
        report['passed'] = report['catalog_implementation'] == 'hive' and all(report['checks'].values())
        if not report['passed']:
            raise ValueError('Catalog/partition checks failed: ' + str(report['checks']))
    except Exception as failure:
        report['error'] = str(failure)
        raise
    finally:
        (output / (args.mode + '-report.json')).write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + '\n', encoding='utf-8')
        spark.stop()
    print(json.dumps({'passed': report['passed'], 'mode': args.mode, 'checks': report['checks']}), flush=True)


if __name__ == '__main__':
    main()
