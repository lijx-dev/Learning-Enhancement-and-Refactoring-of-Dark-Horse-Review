#!/usr/bin/env python3
"""Verify published Parquet models against independently written fixture expectations."""
import argparse
from collections import Counter
from decimal import Decimal
import importlib.util
import json
from pathlib import Path
import sys

import pyarrow.dataset as dataset

PLATFORM = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PLATFORM / 'tools'))
from warehouse import inspect
spec = importlib.util.spec_from_file_location('fixture', PLATFORM / 'tools/fixture.py')
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)


def records(version, name, layer):
    return dataset.dataset(version / layer / name, format='parquet', partitioning='hive').to_table().to_pylist()


def logical_signature(version):
    signature = {}
    for layer in ['dim', 'dwd', 'dws', 'ads']:
        for model in (version / layer).iterdir():
            rows = records(version, model.name, layer)
            signature[model.name] = sorted(json.dumps(row, sort_keys=True, default=str) for row in rows)
    return signature


def verify(root, biz_date, fixture_batch, source_batch=None):
    version, manifest = inspect(root, biz_date)
    if source_batch is not None and manifest['source_batch'] != source_batch:
        raise ValueError('Unexpected source batch')
    wanted = fixture.expected(fixture_batch)
    dws = records(version, 'dws_shop_order_day', 'dws')
    expected_created = {row['shop_id']: row for row in wanted['created'] if row['biz_date'] == biz_date}
    expected_paid = {row['shop_id']: row for row in wanted['paid'] if row['biz_date'] == biz_date}
    if {row['shop_id'] for row in dws} != set(expected_created) | set(expected_paid):
        raise ValueError('DWS shop/date keys differ from hand calculations')
    for row in dws:
        c, p = expected_created.get(row['shop_id'], {}), expected_paid.get(row['shop_id'], {})
        for column, value in {
            'created_orders': c.get('orders', 0), 'created_users': c.get('users', 0),
            'created_known_cents': c.get('known_cents', 0), 'unknown_amount_orders': c.get('unknown_amount', 0),
            'free_orders': c.get('free_orders', 0), 'paid_orders': p.get('orders', 0),
            'paid_known_cents': p.get('known_cents', 0), 'paid_unknown_amount_orders': 0,
            'closed_events': (1 if biz_date == '2026-10-29' else 2) if row['shop_id'] == '1' else 0,
        }.items():
            if row[column] != value:
                raise ValueError(f'{fixture_batch} {row["shop_id"]} {column}: {row[column]} != {value}')
    orders = records(version, 'dwd_order_created', 'dwd')
    events = records(version, 'dwd_order_event', 'dwd')
    if len(orders) != wanted['counts']['orders'] or len(events) != wanted['counts']['history']:
        raise ValueError('DWD counts differ from manual expectations')
    if min(row['order_id'] for row in orders) != '9007199254741001':
        raise ValueError('ID precision lost')
    if sum(row['pay_cents'] or Decimal(0) for row in orders) != (8500 if fixture_batch == 's1' else 24000):
        raise ValueError('DWD cents mismatch')
    status = records(version, 'ads_order_status', 'ads')
    if {r['status']: r['orders'] for r in status} != {r['status']: r['orders'] for r in wanted['status']}:
        raise ValueError('Status snapshot mismatch')
    if manifest['quality']['unprojected_order_events'] != wanted['counts']['missing_projection']:
        raise ValueError('Missing projection count mismatch')
    vouchers = records(version, 'ads_voucher_performance', 'ads')
    if sum(r['created_orders'] for r in vouchers) != sum(r['orders'] for r in expected_created.values()):
        raise ValueError('Voucher cohort grain mismatch')
    ranks = records(version, 'ads_shop_order_rank', 'ads')
    for row in ranks:
        expected_rank = 1 + len({r['created_orders'] for r in ranks if r['created_orders'] > row['created_orders']})
        if row['order_rank'] != expected_rank:
            raise ValueError('Rank mismatch')
    return {'passed': True, 'biz_date': biz_date, 'fixture_batch': fixture_batch,
            'run_id': manifest['run_id'], 'dwd_orders': len(orders), 'dwd_events': len(events),
            'dws_rows': len(dws), 'paid_orders': sum(row['paid_orders'] for row in dws),
            'paid_cents': str(sum(row['paid_known_cents'] or Decimal(0) for row in dws))}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-root', type=Path, required=True)
    parser.add_argument('--biz-date', required=True)
    parser.add_argument('--fixture-batch', required=True)
    args = parser.parse_args()
    print(json.dumps(verify(args.run_root, args.biz_date, args.fixture_batch), ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
