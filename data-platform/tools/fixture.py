#!/usr/bin/env python3
"""Generate and independently check small synthetic source snapshots; never seed hmdp."""
import argparse
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
SPEC = ROOT / 'data-platform/fixtures/hand-calculable'
OUT = ROOT / 'target/warehouse-fixtures/hmdp_hand_v1'
DB = 'hmdp_warehouse_fixture'
OWNER = 'hmdp_hand_v1'
COMPOSE = ['docker', '--context', 'desktop-linux', 'compose', '-f', 'dev/local/compose.yml']


def read_json(path):
    return json.loads(path.read_text(encoding='utf-8'))


def instant(value):
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def timestamp(value):
    return value.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


def expected(batch):
    values = read_json(SPEC / 'expected.json')
    result = values[batch].copy()
    parent = result.pop('same_as', None)
    return {**(expected(parent) if parent else {}), **result}


def queries():
    content = (SPEC / 'metrics.sql').read_text(encoding='utf-8')
    return dict(re.findall(r'-- metric: (\w+)\s*\n(.*?);', content, re.S))


def normalized(rows):
    return [{key: str(value) if value is not None else None for key, value in row.items()} for row in rows]


def assert_result(batch, name, rows):
    wanted = expected(batch)[name]
    if isinstance(wanted, dict):
        wanted = [wanted]
    assert normalized(rows) == normalized(wanted), (batch, name, rows, wanted)


def snapshot(spec, cutoff):
    vouchers = {v['id']: v for v in spec['vouchers']}
    tables = {
        'tb_shop_type': [{'id': '1', 'name': '合成样本餐饮', 'icon': '', 'sort': 1}],
        'tb_shop': [dict(s, type_id='1', images='', address='合成地址', x=120.0, y=30.0,
                         sold=0, comments=0, score=50) for s in spec['shops']],
        'tb_voucher': [], 'tb_voucher_order': [], 'tb_order_event_history': [],
        'tb_business_outbox': [], 'tb_seckill_activity': [],
        'tb_seckill_voucher': [], 'tb_seckill_reservation': [],
    }
    for v in spec['vouchers']:
        changed = 'price_changed_at' in v and instant(v['price_changed_at']) < cutoff
        price = v['changed_price'] if changed else v['pay_value']
        tables['tb_voucher'].append(dict(id=v['id'], shop_id=v['shop_id'], title='合成券' + v['id'],
                                        rules='synthetic only', pay_value=price,
                                        actual_value=price, type=v['type'], status=1))
    for index, case in enumerate(spec['orders'], 1):
        created = instant(case['created'])
        if created >= cutoff:
            continue
        v = vouchers[case['voucher_id']]
        # This v1 scenario holds all known order amounts at the original price.
        # If a new case is after repricing, require an explicit reviewed amount.
        if 'price_changed_at' in v and instant(v['price_changed_at']) <= created:
            assert case.get('amount_snapshot') == v['changed_price'], case['case']
        order = dict(id=case['id'], user_id=case['user_id'], voucher_id=v['id'],
                     pay_type=1, status=2 if v['pay_value'] == 0 else 1,
                     create_time=timestamp(created), update_time=timestamp(created),
                     pay_time=None, use_time=None, refund_time=None,
                     pay_value_snapshot=case.get('amount_snapshot', v['pay_value']),
                     shop_id_snapshot=v['shop_id'], title_snapshot='合成券' + v['id'],
                     rules_snapshot='synthetic only', voucher_type_snapshot=v['type'],
                     expire_at=timestamp(created + timedelta(minutes=30)) if v['type'] == 0 and v['pay_value'] > 0 else None)
        events = [(0, 'ORDER_CREATED', created, created + timedelta(seconds=1))]
        for step, (changed_at, status) in enumerate(case['changes'], 1):
            changed = instant(changed_at)
            if changed >= cutoff:
                continue
            order['status'], order['update_time'] = status, timestamp(changed)
            field = {2: 'pay_time', 3: 'use_time', 6: 'refund_time'}.get(status)
            if field:
                order[field] = timestamp(changed)
            kind = {2: 'ORDER_PAID', 4: 'ORDER_CLOSED'}.get(status)
            if kind:
                processed = instant(case['paid_event_processed']) if kind == 'ORDER_PAID' and 'paid_event_processed' in case else changed + timedelta(seconds=1)
                events.append((step, kind, changed, processed))
        for step, kind, occurred, processed in events:
            eid = str(71000 + index * 10 + step)
            ready = processed < cutoff
            tables['tb_business_outbox'].append(dict(id=eid, kind=kind, aggregate_id=case['id'],
                user_id=case['user_id'], state='DONE' if ready else 'PENDING',
                created_at=timestamp(occurred), consumed_at=timestamp(processed) if ready else None))
            if ready:
                tables['tb_order_event_history'].append(dict(event_id=eid, order_id=case['id'],
                    event_type=kind, occurred_at=timestamp(occurred), processed_at=timestamp(processed)))
        tables['tb_voucher_order'].append(order)
        if v['type'] == 1:
            tables['tb_seckill_reservation'].append(dict(order_id=case['id'], voucher_id=v['id'],
                user_id=case['user_id'], state='SUCCESS', created_at=timestamp(created), updated_at=timestamp(created)))
    reserved = len(tables['tb_seckill_reservation'])
    tables['tb_seckill_activity'] = [dict(voucher_id='21', total_stock=3, has_published=1)]
    tables['tb_seckill_voucher'] = [dict(voucher_id='21', stock=3-reserved,
        begin_time=timestamp(instant('2026-10-29T00:00:00+08:00')),
        end_time=timestamp(instant('2026-10-31T00:00:00+08:00')))]
    # Fix all database defaults to scenario times: repeated imports do not silently use real NOW().
    baseline = '2026-10-28 00:00:00.000'
    for table in ['tb_shop_type', 'tb_shop', 'tb_voucher']:
        for row in tables[table]:
            row['create_time'] = baseline
            row['update_time'] = baseline
    for row in tables['tb_voucher']:
        voucher = vouchers[row['id']]
        if 'price_changed_at' in voucher and instant(voucher['price_changed_at']) < cutoff:
            row['update_time'] = timestamp(instant(voucher['price_changed_at']))
    tables['tb_seckill_activity'][0]['created_at'] = baseline
    tables['tb_seckill_voucher'][0].update(create_time=baseline, update_time=baseline)
    return tables


def generate():
    spec = read_json(SPEC / 'scenario.json')
    assert spec['fixture_id'] == OWNER
    for batch in spec['snapshots']:
        tables = snapshot(spec, instant(batch['as_of']))
        files = {}
        for name, rows in tables.items():
            payload = ''.join(json.dumps(row, ensure_ascii=False, sort_keys=True) + '\n' for row in rows).encode('utf-8')
            files[name + '.jsonl'] = payload
        manifest = dict(fixture_id=OWNER, batch_id=batch['batch_id'], synthetic=True,
            snapshot_semantics='scenario transitions strictly before cutoff, not a real historical DB snapshot',
            scenario_as_of=batch['as_of'], timestamp_timezone='UTC', business_timezone='Asia/Shanghai',
            source_env=spec['source_env'], payment='mock_only', analytics='not_collected',
            files={name: dict(rows=len(payload.splitlines()), sha256=hashlib.sha256(payload).hexdigest()) for name, payload in files.items()})
        files['manifest.json'] = (json.dumps(manifest, ensure_ascii=False, indent=2) + '\n').encode('utf-8')
        folder = OUT / batch['batch_id']
        folder.mkdir(parents=True, exist_ok=True)
        for name, payload in files.items():
            path = folder / name
            if path.exists():
                assert path.read_bytes() == payload, f'Existing frozen fixture differs: {path}; use a new fixture version.'
            else:
                path.write_bytes(payload)
    return spec


def tables_for(batch):
    folder = OUT / batch
    manifest = read_json(folder / 'manifest.json')
    tables = {}
    for name, meta in manifest['files'].items():
        payload = (folder / name).read_bytes()
        assert hashlib.sha256(payload).hexdigest() == meta['sha256'], name
        rows = [json.loads(line) for line in payload.decode('utf-8').splitlines()]
        assert len(rows) == meta['rows'], name
        tables[Path(name).stem] = rows
    return tables


def sqlite_check(batch, tables):
    with sqlite3.connect(':memory:') as db:
        db.row_factory = sqlite3.Row
        for name in ['tb_voucher_order', 'tb_order_event_history', 'tb_business_outbox']:
            rows = tables[name]
            columns = list(rows[0])
            definition = ','.join('"' + col + '" ' + ('INTEGER' if col in ['status', 'pay_value_snapshot'] else 'TEXT') for col in columns)
            db.execute(f'CREATE TABLE {name} ({definition})')
            db.executemany(f'INSERT INTO {name} VALUES ({",".join("?" for _ in columns)})', [[r[c] for c in columns] for r in rows])
        db.execute("CREATE VIEW fixture_orders AS SELECT *, date(create_time,'+8 hours') create_date FROM tb_voucher_order")
        db.execute("CREATE VIEW fixture_history AS SELECT *, date(occurred_at,'+8 hours') event_date FROM tb_order_event_history")
        db.execute('CREATE VIEW fixture_outbox AS SELECT * FROM tb_business_outbox')
        for name, sql in queries().items():
            assert_result(batch, name, [dict(row) for row in db.execute(sql)])
    # Referential checks and exact IDs are independent of the metric totals.
    orders = {r['id']: r for r in tables['tb_voucher_order']}
    assert len(orders) == len(tables['tb_voucher_order'])
    for event in tables['tb_order_event_history']:
        assert event['order_id'] in orders
    assert min(int(key) for key in orders) > 2**53
    if batch != 's1':
        assert orders['9007199254741010']['pay_value_snapshot'] is None
        assert orders['9007199254741001']['pay_value_snapshot'] == 1000
        assert next(v for v in tables['tb_voucher'] if v['id'] == '11')['pay_value'] == 1200


def docker_sql(sql):
    cmd = COMPOSE + ['exec', '-T', 'mysql', 'sh', '-c',
        'export MYSQL_PWD="$MYSQL_ROOT_PASSWORD"; exec mysql -uroot --default-character-set=utf8mb4 --batch --raw']
    proc = subprocess.run(cmd, input=sql, cwd=ROOT, capture_output=True, text=True, encoding='utf-8', timeout=120)
    if proc.returncode:
        raise RuntimeError(proc.stderr)
    return proc.stdout


def sql_literal(value):
    if value is None:
        return 'NULL'
    if isinstance(value, (int, float)):
        return str(value)
    # Hex avoids shell/SQL quote escaping and keeps synthetic Chinese text intact.
    return "CONVERT(X'" + str(value).encode('utf-8').hex() + "' USING utf8mb4)"


def mysql_check(spec):
    report = {}
    exists = docker_sql(f"SELECT SCHEMA_NAME FROM information_schema.schemata WHERE SCHEMA_NAME='{DB}';").strip()
    if exists:
        owner = docker_sql(f'SELECT fixture_id FROM {DB}._fixture_owner;').splitlines()
        if owner != ['fixture_id', OWNER]:
            raise RuntimeError('Refusing to replace a database not owned by this fixture.')
    else:
        docker_sql(f"CREATE DATABASE {DB} CHARACTER SET utf8mb4; CREATE TABLE {DB}._fixture_owner (fixture_id VARCHAR(64) PRIMARY KEY); INSERT INTO {DB}._fixture_owner VALUES ('{OWNER}');")
    # Clone actual migrated source schemas; do not apply simplified mock DDL or modify hmdp.
    before = docker_sql('SELECT (SELECT COUNT(*) FROM hmdp.tb_shop) shops, (SELECT COUNT(*) FROM hmdp.tb_voucher_order) orders;')
    for batch in [item['batch_id'] for item in spec['snapshots']]:
        tables = tables_for(batch)
        for name in tables:
            docker_sql(f'CREATE TABLE IF NOT EXISTS {DB}.{name} LIKE hmdp.{name};')
        cutoff = next(item['as_of'] for item in spec['snapshots'] if item['batch_id'] == batch)
        sql = [f"USE {DB}; SET SESSION time_zone='+00:00'; SET timestamp={int(instant(cutoff).timestamp())}; START TRANSACTION;"]
        for name, rows in tables.items():
            sql.append(f'DELETE FROM {name};')
            for row in rows:
                sql.append(f'INSERT INTO {name} ({",".join(row)}) VALUES ({",".join(sql_literal(v) for v in row.values())});')
        sql.append('COMMIT;')
        docker_sql('\n'.join(sql))
        prefix = f"USE {DB}; SET SESSION time_zone='+00:00';\n"
        views = [
            "CREATE OR REPLACE VIEW fixture_orders AS SELECT *, DATE(CONVERT_TZ(create_time,'+00:00','+08:00')) create_date FROM tb_voucher_order;",
            "CREATE OR REPLACE VIEW fixture_history AS SELECT *, DATE(CONVERT_TZ(occurred_at,'+00:00','+08:00')) event_date FROM tb_order_event_history;",
            'CREATE OR REPLACE VIEW fixture_outbox AS SELECT * FROM tb_business_outbox;'
        ]
        docker_sql(prefix + '\n'.join(views))
        for name, query in queries().items():
            lines = docker_sql(prefix + query + ';').strip().splitlines()
            columns = lines[0].split('\t')
            rows = [dict(zip(columns, line.split('\t'))) for line in lines[1:]]
            assert_result(batch, name, rows)
        report[batch] = expected(batch)['counts']
    after = docker_sql('SELECT (SELECT COUNT(*) FROM hmdp.tb_shop) shops, (SELECT COUNT(*) FROM hmdp.tb_voucher_order) orders;')
    assert before == after, 'Original source counts changed.'
    return dict(database=DB, source_counts_before=before.strip(), source_counts_after=after.strip(), batches=report)


def spark_check(spec):
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.appName('hmdp-hand-calculable-fixture')
        .config('spark.sql.session.timeZone', 'UTC').config('spark.ui.enabled', 'false').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    report = {}
    schemas = {
        'tb_voucher_order': 'id STRING, user_id STRING, shop_id_snapshot STRING, pay_value_snapshot DECIMAL(38,0), status INT, create_time STRING',
        'tb_order_event_history': 'event_id STRING, order_id STRING, event_type STRING, occurred_at STRING',
        'tb_business_outbox': 'id STRING',
    }
    try:
        for item in spec['snapshots']:
            batch = item['batch_id']
            tables_for(batch)  # Verify file integrity before reading.
            for table, schema in schemas.items():
                df = spark.read.schema(schema).json(str(OUT / batch / (table + '.jsonl')))
                df.createOrReplaceTempView(table)
            spark.sql("CREATE OR REPLACE TEMP VIEW fixture_orders AS SELECT *, to_date(from_utc_timestamp(to_timestamp(create_time), 'Asia/Shanghai')) create_date FROM tb_voucher_order")
            spark.sql("CREATE OR REPLACE TEMP VIEW fixture_history AS SELECT *, to_date(from_utc_timestamp(to_timestamp(occurred_at), 'Asia/Shanghai')) event_date FROM tb_order_event_history")
            spark.sql('CREATE OR REPLACE TEMP VIEW fixture_outbox AS SELECT * FROM tb_business_outbox')
            for name, query in queries().items():
                assert_result(batch, name, [row.asDict() for row in spark.sql(query).collect()])
            path = OUT / 'spark-roundtrip' / batch
            for _ in range(2):
                spark.table('fixture_orders').write.mode('overwrite').partitionBy('create_date').parquet(str(path))
                restored = spark.read.parquet(str(path))
                assert restored.count() == expected(batch)['counts']['orders']
                assert restored.selectExpr('min(id) id').first().id == expected(batch)['min_order_id']
                assert restored.selectExpr('sum(pay_value_snapshot) cents').first().cents == sum(
                    (Decimal(row['known_cents'] or 0) for row in expected(batch)['created']), Decimal(0))
            report[batch] = expected(batch)['counts']
        return dict(spark_version=spark.version, batches=report, parquet_repeat_overwrite=True)
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['generate', 'check', 'mysql', 'spark'])
    args = parser.parse_args()
    report_path = OUT / (args.action + '-verification.json')
    report_path.unlink(missing_ok=True)
    spec = generate()
    report = dict(fixture_id=OWNER, action=args.action, synthetic=True)
    if args.action == 'check':
        for item in spec['snapshots']:
            sqlite_check(item['batch_id'], tables_for(item['batch_id']))
        report['checked_batches'] = [b['batch_id'] for b in spec['snapshots']]
    elif args.action == 'mysql':
        report.update(mysql_check(spec))
    elif args.action == 'spark':
        report.update(spark_check(spec))
    report.update(passed=True, verified_at_utc=datetime.now(timezone.utc).isoformat())
    OUT.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
