import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin, urldefrag
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import threading

# --------------- helpers ---------------

USER_AGENT = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/120.0.0.0 Safari/537.36'
)


def get_domain(url: str) -> str:
    return urlparse(url).netloc.replace('www.', '').lower()


def normalize_url(url: str) -> str:
    """Normalize URL for comparison: lowercase, strip www., trailing slash, fragment."""
    url = url.strip().lower()
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    url = urldefrag(url)[0]
    parsed = urlparse(url)
    host = parsed.hostname or ''
    if host.startswith('www.'):
        host = host[4:]
    path = parsed.path.rstrip('/')
    query = f'?{parsed.query}' if parsed.query else ''
    return f'{parsed.scheme}://{host}{path}{query}'


def fetch_page_links(url: str, timeout: int = 15) -> list:
    """Fetch a page and extract ALL <a href> with their anchor texts."""
    resp = requests.get(
        url,
        headers={'User-Agent': USER_AGENT},
        timeout=timeout,
        allow_redirects=True,
    )
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')
    links = []
    for a in soup.find_all('a', href=True):
        href = a['href'].strip()
        absolute_url = urljoin(url, href)
        anchor_text = a.get_text(strip=True)
        links.append({
            'url': absolute_url,
            'normalized_url': normalize_url(absolute_url),
            'anchor': anchor_text,
        })
    return links


# --------------- link check module ---------------

link_check_status = {
    'running': False,
    'total': 0,
    'checked': 0,
    'total_sites': 0,
    'checked_sites': 0,
    'results': [],
    'log': [],
}
_lc_lock = threading.Lock()


def _check_single_site(site_url: str, expected_links: list, timeout: int = 15) -> list:
    """Check one site for expected links. Returns results for each expected link."""
    results = []
    try:
        page_links = fetch_page_links(site_url, timeout)
        url_to_anchors = {}
        for link in page_links:
            norm = link['normalized_url']
            if norm not in url_to_anchors:
                url_to_anchors[norm] = []
            url_to_anchors[norm].append(link['anchor'])

        for expected in expected_links:
            expected_url_norm = normalize_url(expected['link'])
            expected_anchor = expected.get('anchor', '').strip()

            if expected_url_norm in url_to_anchors:
                found_anchors = url_to_anchors[expected_url_norm]
                if not expected_anchor:
                    status = 'ok'
                elif expected_anchor.lower() in [a.lower() for a in found_anchors]:
                    status = 'ok'
                else:
                    status = 'anchor_mismatch'
                results.append({
                    'row_num': expected['row_num'],
                    'site': site_url,
                    'expected_link': expected['link'],
                    'expected_anchor': expected_anchor,
                    'status': status,
                    'found_anchors': found_anchors,
                    'error': None,
                })
            else:
                results.append({
                    'row_num': expected['row_num'],
                    'site': site_url,
                    'expected_link': expected['link'],
                    'expected_anchor': expected.get('anchor', '').strip(),
                    'status': 'link_not_found',
                    'found_anchors': [],
                    'error': None,
                })
    except Exception as e:
        for expected in expected_links:
            results.append({
                'row_num': expected['row_num'],
                'site': site_url,
                'expected_link': expected['link'],
                'expected_anchor': expected.get('anchor', '').strip(),
                'status': 'fetch_error',
                'found_anchors': [],
                'error': str(e)[:200],
            })
    return results


def run_link_check(rows: list, max_threads: int = 5, timeout: int = 15):
    """Orchestrate concurrent link checking via ThreadPoolExecutor."""
    site_groups = {}
    for row in rows:
        site = row['site'].strip()
        if site not in site_groups:
            site_groups[site] = []
        site_groups[site].append(row)

    with _lc_lock:
        link_check_status['running'] = True
        link_check_status['total'] = len(rows)
        link_check_status['checked'] = 0
        link_check_status['total_sites'] = len(site_groups)
        link_check_status['checked_sites'] = 0
        link_check_status['results'] = []
        link_check_status['counts'] = {}
        link_check_status['log'] = []

    all_results = []

    def _work():
        nonlocal all_results
        try:
            with ThreadPoolExecutor(max_workers=max_threads) as executor:
                futures = {}
                for site_url, expected_links in site_groups.items():
                    future = executor.submit(_check_single_site, site_url, expected_links, timeout)
                    futures[future] = site_url

                for future in as_completed(futures):
                    site_url = futures[future]
                    try:
                        site_results = future.result()
                        all_results.extend(site_results)
                        with _lc_lock:
                            link_check_status['checked_sites'] += 1
                            link_check_status['checked'] += len(site_results)
                            link_check_status['log'].append({
                                'site': site_url,
                                'status': 'ok',
                                'ts': datetime.now(timezone.utc).isoformat(),
                            })
                    except Exception as e:
                        with _lc_lock:
                            link_check_status['checked_sites'] += 1
                            link_check_status['log'].append({
                                'site': site_url,
                                'status': 'error',
                                'error': str(e)[:120],
                                'ts': datetime.now(timezone.utc).isoformat(),
                            })

            all_results.sort(key=lambda r: r['row_num'])
            counts = {'ok': 0, 'anchor_mismatch': 0, 'link_not_found': 0, 'fetch_error': 0}
            for r in all_results:
                s = r.get('status', '')
                if s in counts:
                    counts[s] += 1
            with _lc_lock:
                link_check_status['results'] = all_results
                link_check_status['counts'] = counts
        finally:
            with _lc_lock:
                link_check_status['running'] = False

    t = threading.Thread(target=_work, daemon=True)
    t.start()
    return True


# --------------- domain check module ---------------

domain_check_status = {
    'running': False,
    'total': 0,
    'checked': 0,
    'results': [],
    'log': [],
}
_dc_lock = threading.Lock()


def _check_single_domain(domain: str, target_domains: list[str], timeout: int = 15) -> dict:
    """Fetch a domain homepage and check for external links + target domain links."""
    result = {
        'domain': domain,
        'status': 'ok',
        'error': None,
        'links_count': 0,
        'targets': {},
    }
    for td in target_domains:
        result['targets'][td] = {'found': False, 'anchors': []}

    try:
        url = f'https://{domain}/'
        resp = requests.get(
            url,
            headers={'User-Agent': USER_AGENT},
            timeout=timeout,
            allow_redirects=True,
        )
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, 'html.parser')
        source_domain = get_domain(url)

        # collect external links with anchors, grouped by domain
        domain_anchors: dict[str, list[str]] = {}
        ext_count = 0

        for a in soup.find_all('a', href=True):
            href = a['href']
            full_url = urljoin(url, href)
            parsed = urlparse(full_url)
            if parsed.scheme not in ('http', 'https'):
                continue
            link_domain = get_domain(full_url)
            if not link_domain or link_domain == source_domain:
                continue
            ext_count += 1
            anchor = a.get_text(strip=True) or '[no anchor]'
            domain_anchors.setdefault(link_domain, []).append(anchor)

        result['links_count'] = ext_count

        for td in target_domains:
            td_clean = td.replace('www.', '').lower()
            if td_clean in domain_anchors:
                result['targets'][td] = {
                    'found': True,
                    'anchors': domain_anchors[td_clean],
                }

    except Exception as e:
        result['status'] = 'error'
        result['error'] = str(e)[:200]

    return result


def run_domain_check(domains: list[str], target_domains: list[str],
                     max_threads: int = 5, timeout: int = 15):
    """Orchestrate concurrent domain checking via ThreadPoolExecutor."""
    with _dc_lock:
        if domain_check_status['running']:
            return False
        domain_check_status['running'] = True
        domain_check_status['total'] = len(domains)
        domain_check_status['checked'] = 0
        domain_check_status['results'] = []
        domain_check_status['counts'] = {}
        domain_check_status['log'] = []

    all_results = []

    def _work():
        nonlocal all_results
        try:
            with ThreadPoolExecutor(max_workers=max_threads) as executor:
                futures = {}
                for domain in domains:
                    future = executor.submit(_check_single_domain, domain, target_domains, timeout)
                    futures[future] = domain

                for future in as_completed(futures):
                    domain = futures[future]
                    try:
                        result = future.result()
                        all_results.append(result)
                        with _dc_lock:
                            domain_check_status['checked'] += 1
                            domain_check_status['log'].append({
                                'domain': domain,
                                'status': result['status'],
                                'links': result['links_count'],
                                'error': result.get('error', ''),
                                'ts': datetime.now(timezone.utc).isoformat(),
                            })
                    except Exception as e:
                        all_results.append({
                            'domain': domain,
                            'status': 'error',
                            'error': str(e)[:200],
                            'links_count': 0,
                            'targets': {td: {'found': False, 'anchors': []} for td in target_domains},
                        })
                        with _dc_lock:
                            domain_check_status['checked'] += 1
                            domain_check_status['log'].append({
                                'domain': domain,
                                'status': 'error',
                                'error': str(e)[:120],
                                'ts': datetime.now(timezone.utc).isoformat(),
                            })

            all_results.sort(key=lambda r: r['domain'])
            counts = {'ok': 0, 'error': 0}
            for r in all_results:
                s = r.get('status', '')
                if s in counts:
                    counts[s] += 1
            with _dc_lock:
                domain_check_status['results'] = all_results
                domain_check_status['counts'] = counts
        finally:
            with _dc_lock:
                domain_check_status['running'] = False

    t = threading.Thread(target=_work, daemon=True)
    t.start()
    return True
