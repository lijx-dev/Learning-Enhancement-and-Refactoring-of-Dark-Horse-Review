#!/usr/bin/env python3
"""Deterministic synthetic sources for bounded experiments, never a MySQL seed."""
import argparse
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import CONTRACT, TABLES, safe_name, sha256, validate_snapshot, write_json

GENERATOR = 'hmdp-scale-v1'


def generate(root, orders, seed=42):
    if not 1 <= orders <= 100000:
        raise ValueError('Local experiment supports 1..100000 orders')
    if not 0 <= seed <= 2**31 - 1:
        raise ValueError('seed must be in 0..2^31-1')
    batch = safe_name(f'scale-v1-n{orders}-s{seed}')
    folder = Path(root) / batch
    if folder.exists():
        manifest, _ = validate_snapshot(folder, batch)
        if manifest.get('generator') != {'version': GENERATOR, 'orders': orders, 'seed': seed}:
            raise ValueError('Existing source has different generation parameters')
        return folder, batch
    folder.mkdir(parents=True)
    utc = timezone.utc
    start = datetime(2026, 10, 21, tzinfo=timezone(timedelta(hours=8)))

    def stamp(dt):
        return dt.astimezone(utc).isoformat().replace('+00:00', 'Z')

    baseline = stamp(start - timedelta(days=1))
    counts = {table: 0 for table in TABLES}
    with ExitStack() as stack:
        streams = {t: stack.enter_context((folder / (t + '.jsonl')).open('x', encoding='utf-8', newline='\n')) for t in TABLES}

        def emit(table, **values):
            row = {column: values.get(column) for column in CONTRACT['source_tables'][table]}
            streams[table].write(json.dumps(row, ensure_ascii=False, sort_keys=True) + '\n')
            counts[table] += 1

        emit('tb_shop_type', id='1', name='合成餐饮', sort='1', create_time=baseline, update_time=baseline)
        for shop in range(1, 101):
            emit('tb_shop', id=str(shop), name=f'合成店铺{shop}', type_id='1', area='合成区域',
                 address='合成地址', avg_price='50', sold='0', comments='0', score='50',
                 create_time=baseline, update_time=baseline)
            for kind in range(2):
                emit('tb_voucher', id=str(shop * 10 + kind), shop_id=str(shop), title=f'合成券{shop}-{kind}',
                     rules='synthetic only', pay_value='9999', actual_value='9999', type=str(kind), status='1',
                     create_time=baseline, update_time=baseline)
        for i in range(orders):
            # A fixed integer permutation avoids random-library/version dependence.
            mixed = ((i + seed) * 2654435761) & 0xffffffff
            shop = 1 if mixed % 100 < 80 else 2 + (mixed // 100) % 99
            kind = str((mixed >> 8) % 2)
            created = start + timedelta(days=(i // 10) % 10,
                hours=23 if i % 13 == 0 else 9, minutes=50 if i % 13 == 0 else i % 50)
            amount = None if i % 97 == 0 else ('0' if i % 89 == 0 else str(100 + i % 200 * 25))
            paid = i % 10 < 6 and amount != '0'
            closed = i % 10 in (6, 7) and amount != '0'
            changed = created + timedelta(minutes=20)
            oid, uid = str(9007199254741001 + i), str(1001 + (i * 17) % 2000)
            emit('tb_voucher_order', id=oid, user_id=uid, voucher_id=str(shop * 10 + int(kind)),
                 status='2' if paid or amount == '0' else ('4' if closed else '1'),
                 create_time=stamp(created), pay_time=stamp(changed) if paid else None,
                 update_time=stamp(changed) if paid or closed else stamp(created),
                 pay_value_snapshot=amount, shop_id_snapshot=str(shop), title_snapshot='合成历史券',
                 # Exercise source text volume; model SQL intentionally drops this field.
                 rules_snapshot=''.join(f'{(mixed * (j + 1)) & 0xffffffff:08x}' for j in range(32)),
                 voucher_type_snapshot=kind,
                 expire_at=stamp(created + timedelta(minutes=30)) if kind == '0' and amount != '0' else None)
            events = [('ORDER_CREATED', created)]
            if paid or closed:
                events.append(('ORDER_PAID' if paid else 'ORDER_CLOSED', changed))
            for step, (event, occurred) in enumerate(events):
                eid = str(9100000000000000 + i * 2 + step)
                processed = occurred + timedelta(seconds=1)
                emit('tb_order_event_history', event_id=eid, order_id=oid, event_type=event,
                     occurred_at=stamp(occurred), processed_at=stamp(processed))
                emit('tb_business_outbox', id=eid, kind=event, aggregate_id=oid, user_id=uid,
                     state='DONE', created_at=stamp(occurred), consumed_at=stamp(processed))
    write_json(folder / 'manifest.json', {
        'snapshot_version': 'hmdp-offline-v1', 'source_batch': batch,
        'synthetic': True, 'source_env': 'synthetic_scale_experiment', 'payment_provenance': 'mock_only',
        'snapshot_as_of': '2026-11-02T00:00:00Z', 'snapshot_semantics': 'generated scenario, not a historical database snapshot',
        'business_timezone': 'Asia/Shanghai', 'timestamp_storage': 'UTC_ISO8601',
        'generator': {'version': GENERATOR, 'orders': orders, 'seed': seed},
        'source_tables': {t + '.jsonl': {'rows': counts[t], 'sha256': sha256(folder / (t + '.jsonl'))} for t in TABLES}})
    validate_snapshot(folder, batch)
    return folder, batch


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--orders', type=int, required=True)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--root', type=Path, default=Path('target/warehouse-scale/sources'))
    args = parser.parse_args()
    folder, batch = generate(args.root, args.orders, args.seed)
    print(json.dumps({'folder': str(folder), 'source_batch': batch}, ensure_ascii=False))
