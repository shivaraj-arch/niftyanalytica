#!/usr/bin/env python3

import json
import math
import os
import pathlib
import re
import subprocess
import time
import urllib.parse
import tempfile
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from datetime import datetime, timedelta, timezone


ROOT = pathlib.Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / 'data'
OUTPUT_FILE = OUTPUT_DIR / 'market-data.json'
AI_OUTPUT_FILE = OUTPUT_DIR / 'ai-analysis.json'
REQUESTED_AI_MODEL = 'gemini-robotics-er-1.5-preview'
FALLBACK_AI_MODELS = ['gemini-2.5-flash-lite', 'gemini-2.5-flash', 'gemini-2.0-flash']
MARKET_TICKER_SPECS = [
    ('^spx', 'S&P 500'),
    ('^dji', 'Dow Jones'),
    ('^ndq', 'Nasdaq'),
    ('^nkx', 'Nikkei 225'),
    ('^hsi', 'Hang Seng'),
    ('^ukx', 'FTSE 100'),
    ('^dax', 'DAX'),
    ('cl.f', 'Crude Oil'),
    ('xauusd', 'Gold'),
]
NEWS_FEEDS = [
    ('Business Standard', 'https://www.business-standard.com/rss/latest.rss'),
    ('Livemint', 'https://www.livemint.com/rss/news'),
    ('Bloomberg Economics', 'https://feeds.bloomberg.com/economics/news.rss'),
]
MARKET_NEWS_KEYWORDS = (
    'market', 'stock', 'stocks', 'economy', 'economic', 'inflation', 'fed', 'ecb', 'bank', 'oil', 'trade',
    'finance', 'nifty', 'sensex', 'wall street', 'bond', 'currency', 'rupee', 'dollar', 'ipo', 'earnings',
    'shares', 'fii', 'dii', 'rates', 'gdp', 'tariff', 'commodity', 'crude'
)
IST = timezone(timedelta(hours=5, minutes=30))
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Accept': 'application/json,text/plain,*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.nseindia.com/',
}
MONTHS = {
    'Jan': 1,
    'Feb': 2,
    'Mar': 3,
    'Apr': 4,
    'May': 5,
    'Jun': 6,
    'Jul': 7,
    'Aug': 8,
    'Sep': 9,
    'Oct': 10,
    'Nov': 11,
    'Dec': 12,
}


def build_client():
    cookie_file = tempfile.NamedTemporaryFile(prefix='niftyanalytica-nse-', suffix='.txt', delete=False)
    cookie_file.close()
    run_curl('https://www.nseindia.com/', cookie_file.name)
    return cookie_file.name


def run_curl(url, cookie_file):
    command = [
        'curl',
        '-L',
        '-sS',
        '--compressed',
        '-A',
        HEADERS['User-Agent'],
        '-H',
        f"Accept: {HEADERS['Accept']}",
        '-H',
        f"Accept-Language: {HEADERS['Accept-Language']}",
        '-H',
        f"Referer: {HEADERS['Referer']}",
        '-c',
        cookie_file,
        '-b',
        cookie_file,
        url,
    ]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    return result.stdout


def fetch_json(opener, url):
    return json.loads(run_curl(url, opener))


def fetch_text(opener, url):
    return run_curl(url, opener)


def fetch_available_models(api_key):
    url = f'https://generativelanguage.googleapis.com/v1/models?key={api_key}'
    result = subprocess.run(['curl', '-L', '-sS', url], check=True, capture_output=True, text=True)
    payload = json.loads(result.stdout)
    return [item.get('name', '').replace('models/', '') for item in payload.get('models', []) if 'gemini' in item.get('name', '')]


def parse_pub_date(value):
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
        return parsed.astimezone(timezone.utc).isoformat()
    except (TypeError, ValueError, IndexError):
        return None


def parse_number(value):
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = str(value).replace(',', '').replace('%', '').strip()
    if not cleaned:
        return 0.0
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def parse_expiry(expiry):
    if not expiry:
        return datetime.now(IST) + timedelta(days=1)
    parts = expiry.split('-')
    if len(parts) != 3:
        return datetime.now(IST) + timedelta(days=1)
    day, month_name, year = parts
    month = MONTHS.get(month_name)
    if not month:
        return datetime.now(IST) + timedelta(days=1)
    return datetime(int(year), month, int(day), tzinfo=IST)


def norm_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def black_scholes(spot, strike, years, rate, sigma, option_type):
    if sigma <= 0:
        sigma = 0.2
    if years <= 0:
        years = 1.0 / 365.0
    d1 = (math.log(spot / strike) + (rate + 0.5 * sigma * sigma) * years) / (sigma * math.sqrt(years))
    d2 = d1 - sigma * math.sqrt(years)
    if option_type == 'call':
        return spot * norm_cdf(d1) - strike * math.exp(-rate * years) * norm_cdf(d2)
    return strike * math.exp(-rate * years) * norm_cdf(-d2) - spot * norm_cdf(-d1)


def fetch_index_contributors(opener):
    payload = fetch_json(opener, 'https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050')
    rows = payload.get('data', [])
    advance = payload.get('advance', {})
    nifty_row = next((row for row in rows if row.get('symbol') == 'NIFTY 50'), None)
    if not nifty_row:
        raise RuntimeError('NIFTY 50 row missing in contributor payload')

    nifty_previous_close = parse_number(nifty_row.get('previousClose'))
    nifty_ffmc = parse_number(nifty_row.get('ffmc')) or 1.0
    last_price = parse_number(nifty_row.get('lastPrice'))
    transformed = []

    for row in rows:
        ffmc = parse_number(row.get('ffmc'))
        pct_change = parse_number(row.get('pChange'))
        traded_value_cr = parse_number(row.get('totalTradedValue')) / 10000000
        contributing_points = (nifty_previous_close * (ffmc / nifty_ffmc) * pct_change) / 10000000
        if row.get('symbol') == 'NIFTY 50':
            continue

        transformed.append(
            {
                'symbol': row.get('symbol', ''),
                'last': parse_number(row.get('lastPrice')),
                'pChange': pct_change,
                'tradedValueCr': traded_value_cr,
                'contributingPoints': contributing_points,
            }
        )

    transformed.sort(key=lambda row: abs(row['contributingPoints']), reverse=True)
    total_points = sum(row['contributingPoints'] for row in transformed)
    positive_sum = sum(row['contributingPoints'] for row in transformed if row['contributingPoints'] > 0)
    negative_sum = sum(row['contributingPoints'] for row in transformed if row['contributingPoints'] < 0)

    return {
        'timestamp': payload.get('timestamp', ''),
        'lastPrice': last_price,
        'indexChange': parse_number(nifty_row.get('pChange')),
        'totalPoints': total_points,
        'positiveSum': positive_sum,
        'negativeSum': negative_sum,
        'advances': int(advance.get('advances', 0)),
        'declines': int(advance.get('declines', 0)),
        'rows': transformed,
    }


def fetch_option_chain(opener, expiry=None):
    expiry_payload = fetch_json(opener, 'https://www.nseindia.com/api/option-chain-contract-info?symbol=NIFTY')
    expiries = expiry_payload.get('expiryDates', [])
    if not expiries:
        raise RuntimeError('No expiry dates returned by NSE')
    expiry_to_use = expiry or expiries[0]
    query = urllib.parse.quote(expiry_to_use, safe='')
    option_chain_url = f'https://www.nseindia.com/api/option-chain-v3?type=Indices&symbol=NIFTY&expiry={query}'
    chain_payload = fetch_json(opener, option_chain_url)
    return expiries, chain_payload, expiry_to_use


def extract_open_interest(chain_payload, expiry):
    records = chain_payload.get('records', {})
    data = records.get('data', [])
    spot = parse_number(records.get('underlyingValue'))
    atm = round(spot / 50.0) * 50
    strikes = [atm - 300 + index * 50 for index in range(13)]
    strike_rows = []

    for strike in strikes:
        ce = next((item.get('CE') for item in data if item.get('CE', {}).get('strikePrice') == strike), None) or {}
        pe = next((item.get('PE') for item in data if item.get('PE', {}).get('strikePrice') == strike), None) or {}
        strike_rows.append(
            {
                'strikePrice': strike,
                'callOI': parse_number(ce.get('openInterest')),
                'callOIChange': parse_number(ce.get('changeinOpenInterest')),
                'callBidAsk': parse_number(ce.get('totalBuyQuantity')) - parse_number(ce.get('totalSellQuantity')),
                'callIV': parse_number(ce.get('impliedVolatility')),
                'putOI': parse_number(pe.get('openInterest')),
                'putOIChange': parse_number(pe.get('changeinOpenInterest')),
                'putBidAsk': parse_number(pe.get('totalBuyQuantity')) - parse_number(pe.get('totalSellQuantity')),
                'putIV': parse_number(pe.get('impliedVolatility')),
            }
        )

    return {
        'timestamp': records.get('timestamp', ''),
        'spot': spot,
        'expiry': expiry,
        'strikes': strike_rows,
        'rawRecords': data,
    }


def build_black_scholes(open_interest_payload):
    raw_records = open_interest_payload.pop('rawRecords')
    spot = open_interest_payload['spot']
    expiry = open_interest_payload['expiry']
    expiry_date = parse_expiry(expiry)
    today = datetime.now(IST).replace(hour=0, minute=0, second=0, microsecond=0)
    days_to_expiry = max((expiry_date - today).days, 1)
    years = days_to_expiry / 365.0
    rate = 0.1
    rows = []

    for strike_row in open_interest_payload['strikes']:
        strike = strike_row['strikePrice']
        ce = next((item.get('CE') for item in raw_records if item.get('CE', {}).get('strikePrice') == strike), None) or {}
        pe = next((item.get('PE') for item in raw_records if item.get('PE', {}).get('strikePrice') == strike), None) or {}
        call_iv = parse_number(ce.get('impliedVolatility')) / 100.0 or 0.2
        put_iv = parse_number(pe.get('impliedVolatility')) / 100.0 or 0.2
        rows.append(
            {
                'strikePrice': strike,
                'call': {
                    'marketPrice': parse_number(ce.get('lastPrice')),
                    'bsValue': black_scholes(spot, strike, years, rate, call_iv, 'call'),
                    'iv': parse_number(ce.get('impliedVolatility')),
                },
                'put': {
                    'marketPrice': parse_number(pe.get('lastPrice')),
                    'bsValue': black_scholes(spot, strike, years, rate, put_iv, 'put'),
                    'iv': parse_number(pe.get('impliedVolatility')),
                },
            }
        )

    return {
        'timestamp': open_interest_payload['timestamp'],
        'spot': spot,
        'expiry': expiry,
        'rows': rows,
    }


def build_low_iv(opener, expiries):
    rows_by_expiry = {}
    latest_timestamp = ''

    for expiry in expiries[:4]:
        _, chain_payload, _ = fetch_option_chain(opener, expiry=expiry)
        records = chain_payload.get('records', {})
        latest_timestamp = records.get('timestamp', latest_timestamp)
        spot = parse_number(records.get('underlyingValue'))
        expiry_rows = []

        for item in records.get('data', []):
            strike = item.get('CE', {}).get('strikePrice') or item.get('PE', {}).get('strikePrice')
            if not strike or abs(strike - spot) > 1000:
                continue

            for option_type, option_key in (('CE', 'CE'), ('PE', 'PE')):
                contract = item.get(option_key)
                if not contract:
                    continue
                iv = parse_number(contract.get('impliedVolatility'))
                volume = parse_number(contract.get('totalTradedVolume'))
                if iv <= 0 or volume <= 0:
                    continue
                expiry_rows.append(
                    {
                        'type': option_type,
                        'strike': strike,
                        'expiry': expiry,
                        'iv': iv,
                        'oiChangePercent': parse_number(contract.get('pchangeinOpenInterest')),
                        'lastPrice': parse_number(contract.get('lastPrice')),
                        'oi': parse_number(contract.get('openInterest')),
                    }
                )

        expiry_rows.sort(key=lambda row: row['iv'])
        rows_by_expiry[expiry] = expiry_rows[:10]

    default_expiry = next((expiry for expiry in expiries[:4] if rows_by_expiry.get(expiry)), expiries[0] if expiries else '')
    return {
        'timestamp': latest_timestamp,
        'expiries': expiries[:4],
        'defaultExpiry': default_expiry,
        'rowsByExpiry': rows_by_expiry,
    }


def fetch_finance_headlines(opener):
    rss = fetch_text(opener, 'https://news.google.com/rss/search?q=Nifty+50+Indian+stock+market&hl=en-IN&gl=IN&ceid=IN:en')
    root = ET.fromstring(rss)
    headlines = []
    for item in root.findall('.//item')[:5]:
        title = (item.findtext('title') or '').strip()
        cleaned = re.sub(r'\s*-\s*[^-]+$', '', title).strip()
        if cleaned:
            headlines.append(cleaned)
    return headlines


def is_market_story(title, description):
    haystack = f'{title} {description}'.lower()
    return any(keyword in haystack for keyword in MARKET_NEWS_KEYWORDS)


def fetch_news_feed(opener):
    items = []
    seen_titles = set()
    source_errors = []
    sources_available = []

    for source, url in NEWS_FEEDS:
        try:
            xml_text = fetch_text(opener, url)
            if 'Access Denied' in xml_text or not xml_text.lstrip().startswith('<?xml'):
                raise RuntimeError('Feed blocked or non-XML response')
            root = ET.fromstring(xml_text)
            sources_available.append(source)
            for item in root.findall('.//item'):
                title = (item.findtext('title') or '').strip()
                description = (item.findtext('description') or '').strip()
                link = (item.findtext('link') or '').strip()
                if not title or title.lower() in seen_titles:
                    continue
                if not is_market_story(title, description):
                    continue
                seen_titles.add(title.lower())
                items.append(
                    {
                        'source': source,
                        'title': title,
                        'link': link,
                        'publishedAt': parse_pub_date(item.findtext('pubDate')),
                    }
                )
        except (RuntimeError, ET.ParseError, subprocess.CalledProcessError) as exc:
            source_errors.append({'source': source, 'error': str(exc)})

    items.sort(key=lambda item: item.get('publishedAt') or '', reverse=True)
    return {
        'updatedAt': datetime.now(IST).isoformat(),
        'items': items[:10],
        'sourcesAvailable': sources_available,
        'sourceErrors': source_errors,
    }


def fetch_market_tape(contributors):
    items = []
    items.append(
        {
            'label': 'Nifty 50',
            'last': contributors['lastPrice'],
            'changePercent': contributors.get('indexChange', 0.0),
            'source': 'NSE',
            'updatedAt': contributors['timestamp'],
        }
    )

    for symbol, label in MARKET_TICKER_SPECS:
        quote = subprocess.run(
            ['curl', '-L', '-sS', f'https://stooq.com/q/l/?s={symbol}&f=sd2t2ohlcvn&p=1&i=d'],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        if not quote:
            continue
        parts = quote.split(',')
        if len(parts) < 7 or parts[1] == 'N/D':
            continue
        open_price = parse_number(parts[3])
        close_price = parse_number(parts[6])
        change_percent = ((close_price - open_price) / open_price * 100) if open_price else 0.0
        items.append(
            {
                'label': label,
                'last': close_price,
                'changePercent': change_percent,
                'source': 'Stooq',
                'updatedAt': f'{parts[1]} {parts[2]}',
            }
        )

    return {
        'updatedAt': datetime.now(IST).isoformat(),
        'items': items,
    }


def fetch_fii_dii(opener):
    payload = fetch_json(opener, 'https://www.nseindia.com/api/fiidiiTradeReact')
    rows = payload.get('data', []) if isinstance(payload, dict) else payload
    fii_row = next((row for row in rows if row.get('category') == 'FII/FPI'), {})
    dii_row = next((row for row in rows if row.get('category') == 'DII'), {})
    return {
        'date': fii_row.get('date') or dii_row.get('date') or '',
        'fiiNet': parse_number(fii_row.get('netValue')),
        'diiNet': parse_number(dii_row.get('netValue')),
    }


def market_status():
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return 'closed'
    current_minutes = now.hour * 60 + now.minute
    if 9 * 60 + 15 <= current_minutes <= 15 * 60 + 30:
        return 'open'
    return 'closed'


def extract_json_object(text):
    cleaned = text.strip()
    fenced = re.search(r'```(?:json)?\s*(\{.*\})\s*```', cleaned, re.DOTALL)
    if fenced:
        cleaned = fenced.group(1)
    match = re.search(r'\{.*\}', cleaned, re.DOTALL)
    if not match:
        raise ValueError('No JSON object found in model response')
    return json.loads(match.group(0))


def generate_content_with_model(model_name, api_key, prompt):
    url = f'https://generativelanguage.googleapis.com/v1/models/{model_name}:generateContent?key={api_key}'
    payload = {
        'contents': [{'role': 'user', 'parts': [{'text': prompt}]}],
        'generationConfig': {'temperature': 0.4},
    }
    request = subprocess.run(
        [
            'curl', '-L', '-sS', '--compressed', '-X', 'POST', url,
            '-H', 'Content-Type: application/json',
            '-d', json.dumps(payload),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    response = json.loads(request.stdout)
    if 'error' in response:
        raise RuntimeError(response['error'].get('message', f'Gemini error for {model_name}'))
    return response['candidates'][0]['content']['parts'][0]['text']


def generate_ai_analysis(opener, contributors, open_interest, fii_dii, news_feed):
    api_key = os.environ.get('GOOGLE_API_KEY')
    headlines = [item['title'] for item in news_feed.get('items', [])[:5]] or fetch_finance_headlines(opener)
    top_movers = contributors['rows'][:8]
    if not api_key:
        return {
            'generatedAt': datetime.now(IST).isoformat(),
            'model': None,
            'requestedModel': REQUESTED_AI_MODEL,
            'status': 'missing_api_key',
            'summary': 'AI analysis is unavailable because GOOGLE_API_KEY is not configured for this Pages build.',
            'headlines': headlines,
            'fiiDii': fii_dii,
        }

    prompt = f"""You are generating the Nifty Analytica market brief for a static dashboard and mobile app.

Current market status: {market_status()}
Nifty spot: {open_interest['spot']:.2f}
Option expiry: {open_interest['expiry']}
Index timestamp: {contributors['timestamp']}
FII/DII date: {fii_dii['date']}
FII net flow (Cr): {fii_dii['fiiNet']:.2f}
DII net flow (Cr): {fii_dii['diiNet']:.2f}
Advance/Decline: {contributors['advances']} / {contributors['declines']}
Total contributing points: {contributors['totalPoints']:.2f}

Top contributing stocks:
{json.dumps(top_movers, indent=2)}

Important option-chain pressure levels:
{json.dumps(open_interest['strikes'], indent=2)}

Recent finance headlines:
{json.dumps(headlines, indent=2)}

Write JSON only with these keys:
- summary: 180-240 words, professional, actionable, mention bullish/bearish/neutral bias.
- bias: one of Bullish, Bearish, Neutral.
- keyLevels: array of 3 short strings.
- watchlist: array of 4 stock symbols from the provided data.
"""

    available_models = fetch_available_models(api_key)
    models_to_try = [REQUESTED_AI_MODEL] + [model for model in FALLBACK_AI_MODELS if model in available_models]
    last_error = None
    parsed = None
    model_used = None

    for model_name in models_to_try:
        try:
            text = generate_content_with_model(model_name, api_key, prompt)
            parsed = extract_json_object(text)
            model_used = model_name
            break
        except (RuntimeError, ValueError, KeyError, IndexError, subprocess.CalledProcessError, json.JSONDecodeError) as exc:
            last_error = str(exc)

    if parsed is None:
        return {
            'generatedAt': datetime.now(IST).isoformat(),
            'model': None,
            'requestedModel': REQUESTED_AI_MODEL,
            'status': 'model_error',
            'summary': f'AI analysis could not be generated automatically. Last model error: {last_error}',
            'bias': 'Neutral',
            'keyLevels': [],
            'watchlist': [row['symbol'] for row in top_movers[:4]],
            'headlines': headlines,
            'fiiDii': fii_dii,
        }

    return {
        'generatedAt': datetime.now(IST).isoformat(),
        'model': model_used,
        'requestedModel': REQUESTED_AI_MODEL,
        'status': 'ok',
        'summary': parsed.get('summary', '').strip(),
        'bias': parsed.get('bias', 'Neutral').strip(),
        'keyLevels': parsed.get('keyLevels', []),
        'watchlist': parsed.get('watchlist', []),
        'headlines': headlines,
        'fiiDii': fii_dii,
    }


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    opener = build_client()
    contributors = fetch_index_contributors(opener)
    expiries, chain_payload, active_expiry = fetch_option_chain(opener)
    open_interest = extract_open_interest(chain_payload, active_expiry)
    black_scholes_payload = build_black_scholes(dict(open_interest))
    low_iv = build_low_iv(opener, expiries)
    fii_dii = fetch_fii_dii(opener)
    news_feed = fetch_news_feed(opener)
    market_tape = fetch_market_tape(contributors)
    ai_analysis = generate_ai_analysis(opener, contributors, open_interest, fii_dii, news_feed)
    payload = {
        'generatedAt': datetime.now(IST).isoformat(),
        'aiAnalysis': ai_analysis,
        'indexContributors': contributors,
        'openInterest': open_interest,
        'blackScholes': black_scholes_payload,
        'lowIV': low_iv,
        'marketTape': market_tape,
        'newsFeed': news_feed,
    }
    OUTPUT_FILE.write_text(json.dumps(payload, indent=2), encoding='utf-8')
    AI_OUTPUT_FILE.write_text(json.dumps(ai_analysis, indent=2), encoding='utf-8')
    print(f'Wrote {OUTPUT_FILE}')
    print(f'Wrote {AI_OUTPUT_FILE}')


if __name__ == '__main__':
    for attempt in range(3):
        try:
            main()
            break
        except (RuntimeError, ValueError, OSError, subprocess.CalledProcessError, json.JSONDecodeError, ET.ParseError) as exc:
            if attempt == 2:
                raise
            print(f'Retry {attempt + 1} after error: {exc}')
            time.sleep(2)