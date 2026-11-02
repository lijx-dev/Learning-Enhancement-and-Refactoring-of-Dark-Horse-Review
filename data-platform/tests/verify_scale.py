#!/usr/bin/env python3
"""Independent Python oracle over serialized sources; no model SQL or Spark used."""
import argparse
from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))


def day(value):
    # UTC generated source to Shanghai without depending on host zoneinfo data.
    from datetime import timedelta, timezone
    return datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(timezone(timedelta(hours=8))).date().isoformat()


def oracle(source):
    def load(table):
        with (Path(source) / (table + '.jsonl')).open(encoding='utf-8') as stream:
            yield from map(json.loads, stream)
    metrics = ['created_orders', 'created_users', 'created_known_cents', 'unknown_amount_orders',
               'free_orders', 'paid_orders', 'paid_known_cents', 'paid_unknown_amount_orders', 'closed_events']
    dws = defaultdict(lambda: dict.fromkeys(metrics, 0))
    known_created, known_paid = Counter(), Counter()
    known_cohort = Counter()
    users, orders, status, cohort = defaultdict(set), {}, Counter(), {}
    total_cents = 0
    for row in load('tb_voucher_order'):
        orders[row['id']] = row
        status[int(row['status'])] += 1
        key = (day(row['create_time']), row['shop_id_snapshot'])
        group = dws[key]
        amount = None if row['pay_value_snapshot'] is None else int(row['pay_value_snapshot'])
        group['created_orders'] += 1
        group['created_known_cents'] += amount or 0
        known_created[key] += amount is not None
        group['unknown_amount_orders'] += amount is None
        group['free_orders'] += amount == 0
        users[key].add(row['user_id'])
        total_cents += amount or 0
        ck = (*key, row['voucher_id'])
        if ck not in cohort:
            cohort[ck] = dict.fromkeys(['created_orders', 'created_known_cents', 'unknown_amount_orders',
                'cancelled_as_of', 'paid_ordinary_cohort_orders', 'paid_ordinary_cancelled_as_of'], 0)
        c = cohort[ck]
        c['created_orders'] += 1
        c['created_known_cents'] += amount or 0
        known_cohort[ck] += amount is not None
        c['unknown_amount_orders'] += amount is None
        c['cancelled_as_of'] += row['status'] == '4'
        ordinary = row['voucher_type_snapshot'] == '0' and amount is not None and amount > 0
        c['paid_ordinary_cohort_orders'] += ordinary
        c['paid_ordinary_cancelled_as_of'] += ordinary and row['status'] == '4'
    event_count = 0
    for event in load('tb_order_event_history'):
        event_count += 1
        order = orders[event['order_id']]
        if event['event_type'] == 'ORDER_CREATED':
            continue
        key = (day(event['occurred_at']), order['shop_id_snapshot'])
        group = dws[key]
        if event['event_type'] == 'ORDER_PAID':
            amount = order['pay_value_snapshot']
            group['paid_orders'] += 1
            group['paid_known_cents'] += int(amount) if amount is not None else 0
            known_paid[key] += amount is not None
            group['paid_unknown_amount_orders'] += amount is None
        else:
            group['closed_events'] += 1
    for key, values in users.items():
        dws[key]['created_users'] = len(values)
    # SUM of an existing all-NULL group is NULL, while an absent group is zero.
    for key, group in dws.items():
        if group['created_orders'] and not known_created[key]:
            group['created_known_cents'] = None
        if group['paid_orders'] and not known_paid[key]:
            group['paid_known_cents'] = None
    for key, group in cohort.items():
        if not known_cohort[key]:
            group['created_known_cents'] = None
    return dws, cohort, status, len(orders), event_count, total_cents


def verify(source, root, biz_date):
    from warehouse import inspect
    from verify_models import records
    version, manifest = inspect(root, biz_date)
    wanted, cohorts, statuses, count, events, cents = oracle(source)
    actual = records(version, 'dws_shop_order_day', 'dws')
    expected = {key[1]: row for key, row in wanted.items() if key[0] == biz_date}
    if {r['shop_id'] for r in actual} != set(expected):
        raise ValueError('DWS shop keys differ from independent oracle')
    for row in actual:
        for metric, value in expected[row['shop_id']].items():
            if row[metric] != value:
                raise ValueError(f'DWS mismatch {row["shop_id"]} {metric}: {row[metric]} != {value}')
    order_rows = records(version, 'dwd_order_created', 'dwd')
    if len(order_rows) != count or len({r['order_id'] for r in order_rows}) != count:
        raise ValueError('DWD order count/key mismatch')
    if {r['order_id'] for r in order_rows} != {str(9007199254741001 + i) for i in range(count)}:
        raise ValueError('DWD exact ID set mismatch')
    if sum((r['pay_cents'] or Decimal(0)) for r in order_rows) != cents:
        raise ValueError('DWD exact cents mismatch')
    if len(records(version, 'dwd_order_event', 'dwd')) != events:
        raise ValueError('Event count mismatch')
    if {r['status']: r['orders'] for r in records(version, 'ads_order_status', 'ads')} != dict(statuses):
        raise ValueError('Status counts mismatch')
    ads = records(version, 'ads_voucher_performance', 'ads')
    expected_cohort = {key: value for key, value in cohorts.items() if key[0] == biz_date}
    if len(ads) != len(expected_cohort):
        raise ValueError('Cohort key count mismatch')
    for row in ads:
        key = (str(row['biz_date']), row['shop_id'], row['voucher_id'])
        for metric, value in expected_cohort[key].items():
            if row[metric] != value:
                raise ValueError(f'Cohort mismatch {key} {metric}')
    for row in records(version, 'ads_shop_order_rank', 'ads'):
        rank = 1 + len({r['created_orders'] for r in actual if r['created_orders'] > row['created_orders']})
        if row['order_rank'] != rank or row['created_orders'] != expected[row['shop_id']]['created_orders']:
            raise ValueError('Rank mismatch')
    return {'passed': True, 'run_id': manifest['run_id'], 'orders': count, 'events': events,
            'known_cents': str(cents), 'dws_rows': len(actual), 'cohort_rows': len(ads), 'biz_date': biz_date}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--run-root', type=Path, required=True)
    parser.add_argument('--biz-date', required=True)
    args = parser.parse_args()
    print(json.dumps(verify(args.source, args.run_root, args.biz_date), ensure_ascii=False, indent=2))
