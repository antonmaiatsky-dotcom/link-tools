import os
import io
import csv

from flask import Flask, request, jsonify, render_template, Response

from checker import (
    run_link_check, link_check_status,
    run_domain_check, domain_check_status,
)

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'link-tools-secret')

PER_PAGE = 100


# ─── Pages ───────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')


# ─── API: Link Check ─────────────────────────────────────────────────────────

@app.route('/api/link-check/start', methods=['POST'])
def api_link_check_start():
    data = request.get_json(force=True)
    raw_csv = data.get('csv', '')
    max_threads = int(data.get('threads', 5))
    timeout = int(data.get('timeout', 15))

    rows = []
    reader = csv.reader(io.StringIO(raw_csv.strip()))
    for line_num, row in enumerate(reader, 1):
        if len(row) < 2:
            continue
        site = row[0].strip()
        link = row[1].strip()
        anchor = row[2].strip() if len(row) > 2 else ''
        if not site or not link:
            continue
        if not site.startswith(('http://', 'https://')):
            site = 'https://' + site
        rows.append({'site': site, 'link': link, 'anchor': anchor, 'row_num': line_num})

    if not rows:
        return jsonify({'error': 'No valid rows found in CSV'}), 400

    if link_check_status.get('running'):
        return jsonify({'error': 'Link check already in progress'}), 409

    run_link_check(rows, max_threads=max_threads, timeout=timeout)
    return jsonify({'status': 'started', 'count': len(rows)})


@app.route('/api/link-check/status')
def api_link_check_status():
    """Lightweight status — no results array."""
    return jsonify({
        'running': link_check_status['running'],
        'total': link_check_status['total'],
        'checked': link_check_status['checked'],
        'total_sites': link_check_status['total_sites'],
        'checked_sites': link_check_status['checked_sites'],
        'counts': link_check_status.get('counts', {}),
        'log': link_check_status['log'],
    })


@app.route('/api/link-check/stop', methods=['POST'])
def api_link_check_stop():
    link_check_status['running'] = False
    return jsonify({'ok': True})


def _filter_lc(results, f):
    if f and f != 'all':
        return [r for r in results if r.get('status') == f]
    return results


@app.route('/api/link-check/results')
def api_link_check_results():
    """Paginated results. per_page=0 → return all (for CSV export)."""
    page = max(1, request.args.get('page', 1, type=int))
    per_page = request.args.get('per_page', PER_PAGE, type=int)
    f = request.args.get('filter', 'all')

    all_results = link_check_status.get('results', [])
    filtered = _filter_lc(all_results, f)
    total = len(filtered)

    if per_page <= 0:
        return jsonify({'page': 1, 'per_page': total, 'total': total,
                        'total_pages': 1, 'results': filtered})

    total_pages = max(1, (total + per_page - 1) // per_page)
    page = min(page, total_pages)
    start = (page - 1) * per_page
    page_results = filtered[start:start + per_page]

    return jsonify({
        'page': page,
        'per_page': per_page,
        'total': total,
        'total_pages': total_pages,
        'results': page_results,
    })


# ─── API: Domain Check ───────────────────────────────────────────────────────

@app.route('/api/domain-check/start', methods=['POST'])
def api_domain_check_start():
    data = request.get_json(force=True)
    raw_domains = data.get('domains', '')
    raw_targets = data.get('targets', '')
    max_threads = int(data.get('threads', 5))
    timeout = int(data.get('timeout', 15))

    domains = []
    for line in raw_domains.replace(',', '\n').split('\n'):
        d = line.strip().lower().replace('https://', '').replace('http://', '').replace('www.', '').rstrip('/')
        if d:
            domains.append(d)

    targets = []
    for line in raw_targets.replace(',', '\n').split('\n'):
        d = line.strip().lower().replace('https://', '').replace('http://', '').replace('www.', '').rstrip('/')
        if d:
            targets.append(d)

    if not domains:
        return jsonify({'error': 'No referring domains provided'}), 400

    if domain_check_status.get('running'):
        return jsonify({'error': 'Domain check already in progress'}), 409

    run_domain_check(domains, targets, max_threads=max_threads, timeout=timeout)
    return jsonify({'status': 'started', 'count': len(domains), 'targets': len(targets)})


@app.route('/api/domain-check/status')
def api_domain_check_status():
    """Lightweight status — no results array."""
    return jsonify({
        'running': domain_check_status['running'],
        'total': domain_check_status['total'],
        'checked': domain_check_status['checked'],
        'counts': domain_check_status.get('counts', {}),
        'log': domain_check_status['log'],
    })


@app.route('/api/domain-check/stop', methods=['POST'])
def api_domain_check_stop():
    domain_check_status['running'] = False
    return jsonify({'ok': True})


def _dc_target_found(r, td):
    t = r.get('targets', {}).get(td)
    if not t:
        return False
    if isinstance(t, dict):
        return t.get('found', False)
    return bool(t)


def _filter_dc(results, f, target_domains):
    if f == 'ok':
        return [r for r in results if r.get('status') == 'ok']
    if f == 'error':
        return [r for r in results if r.get('status') == 'error']
    if f == 'has_target':
        return [r for r in results if r.get('status') == 'ok'
                and any(_dc_target_found(r, td) for td in target_domains)]
    if f == 'no_target':
        return [r for r in results if r.get('status') == 'ok'
                and not any(_dc_target_found(r, td) for td in target_domains)]
    return results


@app.route('/api/domain-check/results')
def api_domain_check_results():
    """Paginated results. per_page=0 → return all (for CSV export)."""
    page = max(1, request.args.get('page', 1, type=int))
    per_page = request.args.get('per_page', PER_PAGE, type=int)
    f = request.args.get('filter', 'all')
    raw_targets = request.args.get('targets', '')

    target_domains = [t.strip() for t in raw_targets.split(',') if t.strip()] if raw_targets else []

    all_results = domain_check_status.get('results', [])
    filtered = _filter_dc(all_results, f, target_domains)
    total = len(filtered)

    if per_page <= 0:
        return jsonify({'page': 1, 'per_page': total, 'total': total,
                        'total_pages': 1, 'results': filtered})

    total_pages = max(1, (total + per_page - 1) // per_page)
    page = min(page, total_pages)
    start = (page - 1) * per_page
    page_results = filtered[start:start + per_page]

    return jsonify({
        'page': page,
        'per_page': per_page,
        'total': total,
        'total_pages': total_pages,
        'results': page_results,
    })


# ─── Main ────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5001))
    app.run(debug=True, host='0.0.0.0', port=port)
