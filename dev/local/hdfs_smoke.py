#!/usr/bin/env python3
"""Read verified synthetic DWD mirrors through HDFS and persistent Hive SerDe tables."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'data-platform'))
sys.path.insert(0, str(ROOT / 'data-platform/tools'))
from common import safe_name, sha256
from hive_smoke import literal, pruning_evidence

MODELS = {'orders': ('dwd_order_created', 'biz_date'), 'events': ('dwd_order_event', 'event_date')}
MAX_BYTES = 64 * 1024 * 1024


def source_files(source):
    path = Path(source['path']).resolve()
    if not path.is_relative_to(ROOT / 'target'):
        raise ValueError('Source must be under project target')
    if sha256(path / 'run_manifest.json') != source['manifest_sha256']:
        raise ValueError('Source manifest changed')
    manifest = json.loads((path / 'run_manifest.json').read_text(encoding='utf-8'))
    if (not manifest.get('synthetic') or manifest['source_env'] != 'synthetic_local_demo'
            or manifest['quality']['passed'] is not True or manifest.get('model_contract_version') != 'hmdp-model-v2'):
        raise ValueError('Only verified v2 synthetic sources allowed')
    files = {}
    for name, checksum in manifest['artifacts'].items():
        if name.endswith('.crc'):
            continue
        file = (path / name).resolve()
        if not file.is_relative_to(path) or file.is_symlink() or sha256(file) != checksum:
            raise ValueError('Invalid source artifact: ' + name)
        if any(name.startswith('dwd/' + model + '/') for model, _ in MODELS.values()):
            files[name] = {'sha256': checksum, 'bytes': file.stat().st_size}
    if not files or sum(item['bytes'] for item in files.values()) > MAX_BYTES:
        raise ValueError('Empty or oversized hand fixture')
    return path, files


def query(spark, batch):
    orders, events = batch + '_orders', batch + '_events'
    rows = lambda sql: [r.asDict() for r in spark.sql(sql).collect()]
    return {
        'created_orders': spark.table(orders).count(), 'all_events': spark.table(events).count(),
        'created': rows('SELECT CAST(biz_date AS STRING) biz_date,shop_id,COUNT(*) orders,'
            'COUNT(DISTINCT user_id) users,SUM(pay_cents) known_cents,'
            'SUM(CASE WHEN amount_known THEN 0 ELSE 1 END) unknown_amount,'
            'SUM(CASE WHEN is_free THEN 1 ELSE 0 END) free_orders FROM ' + orders +
            ' GROUP BY biz_date,shop_id ORDER BY biz_date,shop_id'),
        'paid': rows('SELECT CAST(e.event_date AS STRING) biz_date,o.shop_id,COUNT(*) orders,'
            'SUM(o.pay_cents) known_cents,SUM(CASE WHEN o.amount_known THEN 0 ELSE 1 END) unknown_amount '
            'FROM ' + events + ' e JOIN ' + orders + " o ON e.order_id=o.order_id WHERE e.event_type='ORDER_PAID' "
            'GROUP BY e.event_date,o.shop_id ORDER BY e.event_date,o.shop_id'),
        'closed': rows('SELECT CAST(event_date AS STRING) biz_date,COUNT(*) orders FROM ' + events +
            " WHERE event_type='ORDER_CLOSED' GROUP BY event_date ORDER BY event_date"),
        'partitions': {table: sorted(r[0] for r in spark.sql('SHOW PARTITIONS ' + batch + '_' + table).collect())
            for table in MODELS}}


def main():
    from pyspark.sql import SparkSession
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inputs', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--mode', choices=['build', 'reopen'], required=True)
    args = parser.parse_args()
    inputs = json.loads(args.inputs.read_text(encoding='utf-8'))
    tag = safe_name(inputs['tag'])
    base = 'hdfs://namenode:8020/hmdp-lab/' + tag
    output = args.output.resolve()
    if args.mode == 'build':
        output.mkdir(parents=True, exist_ok=False)
    elif not (output / 'metastore_db').is_dir():
        raise ValueError('Missing persistent Hive catalog')
    spark = (SparkSession.builder.appName('hmdp-hdfs-' + args.mode)
        .config('spark.sql.warehouse.dir', (output / 'warehouse').as_uri())
        .config('spark.hadoop.javax.jdo.option.ConnectionURL',
            'jdbc:derby:;databaseName=' + (output / 'metastore_db').as_posix() + ';create=true')
        .config('spark.sql.session.timeZone', 'UTC').config('spark.sql.ansi.enabled', 'true')
        .config('spark.sql.shuffle.partitions', '2').config('spark.ui.enabled', 'false')
        .enableHiveSupport().getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    jvm = spark.sparkContext._jvm
    jvm.java.lang.System.setProperty('derby.stream.error.file', (output / 'derby.log').as_posix())
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(base), spark.sparkContext._jsc.hadoopConfiguration())
    hp = lambda value: jvm.org.apache.hadoop.fs.Path(value)
    report = {'mode': args.mode, 'spark_version': spark.version,
        'hadoop_client_version': jvm.org.apache.hadoop.util.VersionInfo.getVersion(),
        'catalog_implementation': spark.conf.get('spark.sql.catalogImplementation'),
        'hdfs_root': base, 'snapshots': {}, 'files': {}, 'tables': {}, 'writeback': {}, 'pruning': {}}
    try:
        if fs.getUri().toString() != 'hdfs://namenode:8020':
            raise ValueError('Unexpected filesystem; local fallback is forbidden')
        if args.mode == 'build':
            if fs.exists(hp(base)):
                raise ValueError('HDFS tag already exists; never overwrite')
            fs.mkdirs(hp(base))
            spark.sql('CREATE DATABASE hmdp_hdfs_lab')
        spark.sql('USE hmdp_hdfs_lab')

        def hashes(root, files):
            result = {}
            for name, wanted in files.items():
                remote = hp(root + '/' + name)
                if fs.getFileStatus(remote).getLen() != wanted['bytes']:
                    raise ValueError('HDFS size mismatch: ' + name)
                stream = fs.open(remote)
                try:
                    raw = bytes(jvm.org.apache.commons.io.IOUtils.toByteArray(stream))
                finally:
                    stream.close()
                actual = hashlib.sha256(raw).hexdigest()
                if actual != wanted['sha256']:
                    raise ValueError('HDFS hash mismatch: ' + name)
                result[name] = wanted
            return result

        for batch in ('s2', 's3'):
            source, files = source_files(inputs['sources'][batch])
            mirror = base + '/' + batch
            if args.mode == 'build':
                staging = base + '/.' + batch + '-upload'
                fs.mkdirs(hp(staging))
                for name in files:
                    destination = hp(staging + '/' + name)
                    fs.mkdirs(destination.getParent())
                    fs.copyFromLocalFile(False, False, hp(str(source / name)), destination)
                hashes(staging, files)
                if not fs.rename(hp(staging), hp(mirror)):
                    raise ValueError('HDFS source mirror rename failed')
            report['files'][batch] = hashes(mirror, files)
            report['tables'][batch] = {}
            for table, (model, partition) in MODELS.items():
                uri, name = mirror + '/dwd/' + model, batch + '_' + table
                if args.mode == 'build':
                    schema = spark.read.parquet(uri).schema
                    columns = ','.join('`' + f.name + '` ' + f.dataType.simpleString() for f in schema if f.name != partition)
                    spark.sql(f'CREATE EXTERNAL TABLE {name} ({columns}) PARTITIONED BY ({partition} DATE) '
                        'STORED AS PARQUET LOCATION ' + literal(uri))
                    spark.sql('MSCK REPAIR TABLE ' + name)
                detail = {r.col_name: r.data_type for r in spark.sql('DESCRIBE FORMATTED ' + name).collect()}
                report['tables'][batch][table] = {'location': detail.get('Location'),
                    'type': detail.get('Type'), 'provider': detail.get('Provider'),
                    'schema': spark.table(name).schema.simpleString(),
                    'input_files': sorted(spark.table(name).inputFiles())}
            report['snapshots'][batch] = query(spark, batch)
            plan = spark.sql("EXPLAIN FORMATTED SELECT COUNT(*) FROM " + batch +
                "_events WHERE event_date=DATE '2026-10-30'").first()[0]
            (output / (args.mode + '-' + batch + '-explain.txt')).write_text(plan, encoding='utf-8')
            report['pruning'][batch] = pruning_evidence(plan, '2026-10-30')
            target = base + '/derived/' + batch
            if args.mode == 'build':
                spark.sql('SELECT event_date,COUNT(*) paid_orders FROM ' + batch +
                    "_events WHERE event_type='ORDER_PAID' GROUP BY event_date").write.mode('errorifexists').parquet(target)
            report['writeback'][batch] = {'location': target, 'rows': [r.asDict() for r in
                spark.read.parquet(target).selectExpr('CAST(event_date AS STRING) biz_date', 'paid_orders').orderBy('biz_date').collect()]}
        report['completed'] = True
    except BaseException as failure:
        report['error'] = str(failure)
        raise
    finally:
        (output / (args.mode + '-report.json')).write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + '\n', encoding='utf-8')
        spark.stop()


if __name__ == '__main__':
    main()
