"""Shared source validation and immutable local publication protocol (standard library)."""
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re

PLATFORM = Path(__file__).resolve().parent
CONTRACT = json.loads((PLATFORM / 'contracts/source-contract.json').read_text(encoding='utf-8'))
TABLES = tuple(CONTRACT['source_tables'])
ORDER_KINDS = ('ORDER_CREATED', 'ORDER_PAID', 'ORDER_CLOSED')
KEYS = {'tb_order_event_history': 'event_id', **{name: 'id' for name in TABLES if name != 'tb_order_event_history'}}
IDS = {'id', 'event_id', 'order_id', 'user_id', 'voucher_id', 'shop_id', 'shop_id_snapshot', 'type_id', 'aggregate_id'}
REQUIRED = {
    'tb_shop': ['id', 'name', 'type_id'], 'tb_shop_type': ['id', 'name'],
    'tb_voucher': ['id', 'shop_id', 'pay_value', 'type'],
    'tb_voucher_order': ['id', 'user_id', 'voucher_id', 'status', 'create_time', 'pay_value_snapshot', 'shop_id_snapshot', 'voucher_type_snapshot'],
    'tb_order_event_history': ['event_id', 'order_id', 'event_type', 'occurred_at', 'processed_at'],
    'tb_business_outbox': ['id', 'kind', 'aggregate_id', 'state', 'created_at'],
}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def safe_name(value):
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,79}', value):
        raise ValueError('Invalid batch/run identifier: ' + value)
    return value


def timestamp(value, *, fixture=False):
    if not isinstance(value, str):
        raise ValueError('Timestamp must be a string')
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        if not fixture:
            raise ValueError('Timestamp requires an explicit UTC offset: ' + value)
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def validate_snapshot(folder, source_batch):
    """Reject missing/corrupt/ambiguous records before a Spark session is created."""
    folder = Path(folder).resolve()
    manifest = json.loads((folder / 'manifest.json').read_text(encoding='utf-8'))
    batch = manifest.get('source_batch', manifest.get('batch_id'))
    if batch != safe_name(source_batch):
        raise ValueError('Source batch does not match manifest')
    fixture = manifest.get('fixture_id') == 'hmdp_hand_v1' and manifest.get('synthetic') is True
    if not fixture and manifest.get('snapshot_version') != 'hmdp-offline-v1':
        raise ValueError('Unsupported source manifest version')
    if manifest.get('business_timezone') != 'Asia/Shanghai':
        raise ValueError('Unsupported business timezone')
    if fixture:
        if manifest.get('timestamp_timezone') != 'UTC':
            raise ValueError('Fixture must declare UTC source timestamps')
    elif manifest.get('timestamp_storage') != 'UTC_ISO8601':
        raise ValueError('Source timestamps must be UTC ISO8601')
    as_of = manifest.get('snapshot_as_of', manifest.get('scenario_as_of'))
    timestamp(as_of)
    if not manifest.get('source_env'):
        raise ValueError('source_env is required')
    metadata = manifest.get('source_tables', manifest.get('files', {}))
    result = {}
    for table in TABLES:
        filename = table + '.jsonl'
        meta = metadata.get(filename)
        path = folder / filename
        if not meta or path.is_symlink() or not path.is_file() or sha256(path) != meta['sha256']:
            raise ValueError('Missing or corrupt source file: ' + filename)
        seen, rows, paid_orders = set(), 0, set()
        with path.open(encoding='utf-8') as stream:
            for line in stream:
                row = json.loads(line)
                if not isinstance(row, dict) or any(key not in row for key in REQUIRED[table]):
                    raise ValueError('Missing required columns: ' + table)
                key = row[KEYS[table]]
                if key is None or key in seen:
                    raise ValueError('Null or duplicate primary key: ' + table)
                seen.add(key)
                for column, value in row.items():
                    if value is None:
                        continue
                    if column in IDS:
                        if not isinstance(value, str) or not re.fullmatch(r'0|[1-9][0-9]*', value) or int(value) > 2**64-1:
                            raise ValueError('ID must be an exact unsigned-range decimal string: ' + table + '.' + column)
                    if column in {'pay_value_snapshot', 'pay_value', 'actual_value'}:
                        if isinstance(value, bool) or not re.fullmatch(r'0|[1-9][0-9]*', str(value)) or Decimal(str(value)) > Decimal(2**64-1):
                            raise ValueError('Invalid cents: ' + table + '.' + column)
                    if column.endswith('_time') or column in {'occurred_at', 'processed_at', 'created_at', 'consumed_at', 'expire_at'}:
                        timestamp(value, fixture=fixture)
                if table == 'tb_voucher_order' and str(row['status']) not in ('1', '2', '3', '4', '5', '6'):
                    raise ValueError('Invalid order status')
                if table == 'tb_order_event_history':
                    if row['event_type'] not in ORDER_KINDS:
                        raise ValueError('Unsupported order event type')
                    if row['event_type'] == 'ORDER_PAID':
                        if row['order_id'] in paid_orders:
                            raise ValueError('Multiple paid events for one order require a new business contract')
                        paid_orders.add(row['order_id'])
                rows += 1
        if rows != meta['rows']:
            raise ValueError('Source row count mismatch: ' + filename)
        result[table] = rows
    return manifest, result


@contextmanager
def exclusive_lock(root, key):
    directory = Path(root) / '.locks'
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / (safe_name(key) + '.json')
    with path.open('x', encoding='utf-8') as stream:
        json.dump({'pid': os.getpid(), 'created_at_utc': utc_now()}, stream)
    try:
        yield
    finally:
        path.unlink()


def publish(staging, final, catalog, metadata):
    """Retain immutable versions; only replace the small catalog pointer atomically."""
    final = Path(final)
    if final.exists():
        raise FileExistsError('Run already exists; choose a new run-id')
    final.parent.mkdir(parents=True, exist_ok=True)
    Path(staging).rename(final)
    catalog = Path(catalog)
    catalog.parent.mkdir(parents=True, exist_ok=True)
    temp = catalog.with_name(catalog.name + '.' + safe_name(metadata['run_id']) + '.tmp')
    write_json(temp, metadata)
    os.replace(temp, catalog)
