#!/usr/bin/env python3
"""Export a compact, portable result with hashes linking it to local raw evidence."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import sha256, utc_now, write_json


def summarize(report_path):
    report_path = Path(report_path)
    raw = json.loads(report_path.read_text(encoding='utf-8'))
    if not raw.get('passed') or not all(raw['all_model_cache_equivalence'].values()):
        raise ValueError('Only fully passed evidence can be summarized')
    result = {key: raw[key] for key in ['tag', 'started_at_utc', 'completed_at_utc', 'python', 'host',
        'docker', 'image_id', 'seed', 'biz_date', 'limits', 'source_hashes', 'all_model_cache_equivalence']}
    result['raw_report_sha256'] = sha256(report_path)
    result['exported_at_utc'] = utc_now()
    result['runs'] = raw['runs']
    for run in result['runs']:
        del run['version']  # No personal absolute path; raw report retains local artifact locations.
    result['queries'] = {}
    for size, query in raw['queries'].items():
        if not query.get('passed'):
            raise ValueError('Failed query suite')
        compact = {key: query[key] for key in ['spark_version', 'method', 'query_conf', 'layouts',
            'skew_partition_rows', 'largest_partition_fraction']}
        compact['pairs'] = {}
        for name, pair in query['pairs'].items():
            if not pair['results_equal']:
                raise ValueError('Unequal query result')
            item = {key: pair[key] for key in ['results_equal', 'result_rows', 'result_sha256']}
            item['variants'] = {}
            for variant, data in pair['variants'].items():
                selected_metrics = []
                for execution in data['metrics']:
                    selected_metrics.append({
                        'scan_files': sum(m['values'].get('numFiles', 0) for m in execution),
                        'selected_file_bytes': sum(m['values'].get('filesSize', 0) for m in execution),
                        'shuffle_bytes_written': sum(m['values'].get('shuffleBytesWritten', 0) for m in execution)})
                item['variants'][variant] = {'seconds': data['seconds'], 'median_seconds': data['median_seconds'],
                    'sql_metrics': selected_metrics,
                    'explain_sha256': {Path(p).name: sha256(report_path.parent / ('queries-n' + size) / p) for p in data['plans']}}
            compact['pairs'][name] = item
        result['queries'][size] = compact
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError('Choose a new summary output; do not overwrite evidence')
    result = summarize(args.report)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    write_json(args.output, result)
