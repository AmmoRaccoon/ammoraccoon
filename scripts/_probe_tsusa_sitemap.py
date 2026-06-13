"""Probe: harvest current Target Sports USA category IDs from their sitemap.

2026-06-11 audit: TSUSA renumbered category IDs; our hardcoded URLs 301 to
WRONG calibers (.223 URL lands on .44 Rem Mag, .22 LR URL on .44 Special),
so the strict caliber gate saves zero and 9 of 10 calibers are dark.
Read-only: fetches robots.txt + sitemap, prints every ammo category URL
matching '-c-<id>.aspx', then the candidate mapping for our 10 calibers.
"""
import re
import requests

UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36')
HDRS = {'User-Agent': UA,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9'}
BASE = 'https://www.targetsportsusa.com'

r = requests.get(BASE + '/robots.txt', headers=HDRS, timeout=30)
print(f'robots.txt -> {r.status_code}')
sitemaps = re.findall(r'(?im)^sitemap:\s*(\S+)', r.text)
print('declared sitemaps:', sitemaps or '(none)')
if not sitemaps:
    sitemaps = [BASE + '/sitemap.xml']

cat_urls = []
seen_xml = set()
queue = list(sitemaps)
while queue:
    sm = queue.pop(0)
    if sm in seen_xml:
        continue
    seen_xml.add(sm)
    r = requests.get(sm, headers=HDRS, timeout=60)
    print(f'{sm} -> {r.status_code} ({len(r.text)} bytes)')
    if r.status_code != 200:
        continue
    locs = re.findall(r'<loc>([^<]+)</loc>', r.text)
    subs = [l for l in locs if l.endswith('.xml')]
    queue.extend(subs)
    cat_urls.extend(l for l in locs if re.search(r'-c-\d+\.aspx', l))

cat_urls = sorted(set(cat_urls))
print(f'\n{len(cat_urls)} category URLs in sitemap')
for u in cat_urls:
    print(' ', u)

# Our 10 calibers -> regex over the slug portion of the category URL.
WANT = {
    '9mm':     r'9mm-luger',
    '380acp':  r'380-acp|380-auto',
    '40sw':    r'40-s-?w',
    '38spl':   r'38-special',
    '357mag':  r'357-magnum',
    '22lr':    r'22-lr|22-long-rifle',
    '223-556': r'223-rem|5-56',
    '308win':  r'308-win|7-62x51',
    '762x39':  r'7-62x39',
    '300blk':  r'300-aac|300-blackout',
}
print('\n--- candidate mapping ---')
for cal, pat in WANT.items():
    hits = [u for u in cat_urls if re.search(pat, u, re.I)]
    print(f'{cal}:')
    for h in hits:
        print('   ', h)
    if not hits:
        print('    (NO MATCH IN SITEMAP)')
