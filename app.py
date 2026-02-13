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
    return jsonify(link_check_status)


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
    return jsonify(domain_check_status)


# ─── Main ────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5001))
    app.run(debug=True, host='0.0.0.0', port=port)
