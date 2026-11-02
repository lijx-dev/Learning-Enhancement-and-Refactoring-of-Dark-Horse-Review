#!/usr/bin/env python3
"""Extract a real current MySQL consistent snapshot using one read-only transaction."""
import argparse
from datetime import date, datetime, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import CONTRACT, IDS, ORDER_KINDS, exclusive_lock, safe_name, sha256, utc_now, write_json, validate_snapshot


def json_value(column, value):
    if value is None:
        return None
    if column in IDS or isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')
    if isinstance(value, date):
        return value.isoformat()
    return value


def extract(args):
    import pymysql
    source_batch = safe_name(args.source_batch)
    root = args.output_root.resolve()
    final = root / source_batch
    if final.exists():
        raise FileExistsError('Frozen batch exists: ' + str(final))
    with exclusive_lock(root, source_batch):
        stage = root / '.staging' / source_batch
        stage.mkdir(parents=True, exist_ok=False)
        started = utc_now()
        timer = time.monotonic()
        password = os.environ.get(args.password_env)
        if password is None:
            raise ValueError('Missing password environment variable: ' + args.password_env)
        connection = pymysql.connect(host=args.host, port=args.port, user=args.user, password=password,
            database=args.database, charset='utf8mb4', autocommit=False,
            read_timeout=120, write_timeout=120, connect_timeout=15)
        files, schema = {}, {}
        try:
            with connection.cursor() as cursor:
                for table, columns in CONTRACT['source_tables'].items():
                    cursor.execute('SELECT ENGINE FROM information_schema.tables WHERE table_schema=%s AND table_name=%s', (args.database, table))
                    if cursor.fetchone() != ('InnoDB',):
                        raise ValueError('Requires an InnoDB source table: ' + table)
                    cursor.execute('SELECT COLUMN_NAME,COLUMN_TYPE,DATA_TYPE FROM information_schema.columns WHERE table_schema=%s AND table_name=%s ORDER BY ORDINAL_POSITION', (args.database, table))
                    types = {row[0]: {'column_type': row[1], 'data_type': row[2]} for row in cursor.fetchall()}
                    if any(column not in types for column in columns):
                        raise ValueError('Source schema missing required columns: ' + table)
                    if any(types[column]['data_type'] == 'datetime' for column in columns):
                        raise ValueError('DATETIME needs an explicit source timezone contract')
                    schema[table] = {column: types[column] for column in columns}
                cursor.execute('SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ')
                cursor.execute("SET SESSION time_zone = '+00:00'")
                cursor.execute('START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY')
                acquired = utc_now()
                cursor.execute('SELECT @@session.time_zone, @@transaction_isolation, UTC_TIMESTAMP(6)')
                session_zone, isolation, server_time = cursor.fetchone()
                cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=%s AND table_name='flyway_schema_history'", (args.database,))
                migrations = None
                if cursor.fetchone()[0]:
                    cursor.execute('SELECT version,success FROM flyway_schema_history ORDER BY installed_rank')
                    migrations = [[v, int(ok)] for v, ok in cursor.fetchall()]
                    if any(not ok for _, ok in migrations):
                        raise ValueError('Source has a failed migration')
                for table, columns in CONTRACT['source_tables'].items():
                    if time.monotonic() - timer > args.max_seconds:
                        raise TimeoutError('Source transaction exceeded max-seconds')
                    selected = ','.join('`' + column + '`' for column in columns)
                    where = " WHERE kind IN ('ORDER_CREATED','ORDER_PAID','ORDER_CLOSED')" if table == 'tb_business_outbox' else ''
                    cursor.execute('SELECT COUNT(*) FROM `' + table + '`' + where)
                    source_count = cursor.fetchone()[0]
                    path = stage / (table + '.jsonl')
                    count = 0
                    # Stream on the same connection/transaction; fully drain before any next SELECT.
                    with connection.cursor(pymysql.cursors.SSCursor) as rows_cursor, path.open('w', encoding='utf-8', newline='\n') as stream:
                        rows_cursor.execute('SELECT ' + selected + ' FROM `' + table + '`' + where + ' ORDER BY `' + columns[0] + '`')
                        for row in rows_cursor:
                            if time.monotonic() - timer > args.max_seconds:
                                raise TimeoutError('Source transaction exceeded max-seconds')
                            payload = {column: json_value(column, value) for column, value in zip(columns, row)}
                            stream.write(json.dumps(payload, ensure_ascii=False, separators=(',', ':')) + '\n')
                            count += 1
                    if count != source_count:
                        raise ValueError('Consistent source count mismatch: ' + table)
                    files[path.name] = {'rows': count, 'bytes': path.stat().st_size, 'sha256': sha256(path), 'columns': columns}
                cursor.execute("SELECT COUNT(*) FROM tb_business_outbox b LEFT JOIN tb_order_event_history h ON b.id=h.event_id WHERE b.kind IN ('ORDER_CREATED','ORDER_PAID','ORDER_CLOSED') AND h.event_id IS NULL")
                missing = cursor.fetchone()[0]
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
        manifest = {
            'snapshot_version': 'hmdp-offline-v1', 'source_batch': source_batch,
            'source_env': args.source_env, 'database': args.database, 'synthetic': args.synthetic,
            'snapshot_as_of': acquired, 'extraction_started_at_utc': started, 'extraction_finished_at_utc': utc_now(),
            'as_of_semantics': 'client-observed transaction acquisition time, not a retrospective cutoff or binlog position',
            'server_utc_at_start': json_value('server_time', server_time),
            'business_timezone': 'Asia/Shanghai', 'timestamp_storage': 'UTC_ISO8601', 'id_encoding': 'decimal_string',
            'amount_encoding': 'integer_cents', 'session_timezone': session_zone, 'transaction_isolation': isolation,
            'read_only': True, 'payment_provenance': args.payment_provenance, 'analytics': 'not_collected',
            'source_tables': files, 'source_schema': schema, 'flyway_migrations': migrations,
            'unprojected_order_events': missing, 'elapsed_seconds': round(time.monotonic()-timer, 3),
            'contract_sha256': sha256(Path(__file__).resolve().parents[1] / 'contracts/source-contract.json'),
            'extractor_sha256': sha256(__file__), 'ddl_policy': 'DDL must be excluded by source operator; no global lock acquired',
        }
        write_json(stage / 'manifest.json', manifest)
        validate_snapshot(stage, source_batch)
        stage.rename(final)
        print(json.dumps({'passed': True, 'path': str(final), 'rows': {k: v['rows'] for k, v in files.items()}, 'snapshot_as_of': acquired}, ensure_ascii=False, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default=os.environ.get('HMDP_WAREHOUSE_HOST', '127.0.0.1'))
    parser.add_argument('--port', type=int, default=int(os.environ.get('HMDP_WAREHOUSE_PORT', '13306')))
    parser.add_argument('--database', default='hmdp')
    parser.add_argument('--user', default='warehouse_reader')
    parser.add_argument('--password-env', default='HMDP_WAREHOUSE_PASSWORD')
    parser.add_argument('--output-root', type=Path, default=Path(__file__).resolve().parents[2] / 'target/warehouse-snapshots')
    parser.add_argument('--source-batch', required=True)
    parser.add_argument('--source-env', required=True)
    parser.add_argument('--payment-provenance', choices=['mock_only', 'unknown'], required=True)
    parser.add_argument('--synthetic', action='store_true')
    parser.add_argument('--max-seconds', type=int, default=120)
    extract(parser.parse_args())


if __name__ == '__main__':
    main()
