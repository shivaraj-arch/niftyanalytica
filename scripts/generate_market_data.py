#!/usr/bin/env python3

import json
import math
import pathlib
import subprocess
import time
import urllib.parse
import tempfile
from datetime import datetime, timedelta, timezone


ROOT = pathlib.Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / 'data'
OUTPUT_FILE = OUTPUT_DIR / 'market-data.json'
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
        'totalPoints': total_points,
        'positiveSum': positive_sum,
        'negativeSum': negative_sum,
        'advances': int(advance.get('advances', 0)),
        'declines': int(advance.get('declines', 0)),
        'rows': transformed[:20],
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
    rows = []
    baseline = {}
    latest_timestamp = ''

    for expiry in expiries[:4]:
        _, chain_payload, _ = fetch_option_chain(opener, expiry=expiry)
        records = chain_payload.get('records', {})
        latest_timestamp = records.get('timestamp', latest_timestamp)
        spot = parse_number(records.get('underlyingValue'))

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
                baseline_key = f'{option_type}_{strike}_{expiry}'
                initial_iv = baseline.setdefault(baseline_key, iv)
                rows.append(
                    {
                        'type': option_type,
                        'strike': strike,
                        'expiry': expiry,
                        'iv': iv,
                        'ivChange': iv - initial_iv,
                        'oiChangePercent': parse_number(contract.get('pchangeinOpenInterest')),
                        'lastPrice': parse_number(contract.get('lastPrice')),
                        'oi': parse_number(contract.get('openInterest')),
                    }
                )

    rows.sort(key=lambda row: row['iv'])
    return {
        'timestamp': latest_timestamp,
        'expiries': expiries[:4],
        'rows': rows[:50],
    }


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    opener = build_client()
    contributors = fetch_index_contributors(opener)
    expiries, chain_payload, active_expiry = fetch_option_chain(opener)
    open_interest = extract_open_interest(chain_payload, active_expiry)
    black_scholes_payload = build_black_scholes(dict(open_interest))
    low_iv = build_low_iv(opener, expiries)
    payload = {
        'generatedAt': datetime.now(IST).isoformat(),
        'indexContributors': contributors,
        'openInterest': open_interest,
        'blackScholes': black_scholes_payload,
        'lowIV': low_iv,
    }
    OUTPUT_FILE.write_text(json.dumps(payload, indent=2), encoding='utf-8')
    print(f'Wrote {OUTPUT_FILE}')


if __name__ == '__main__':
    for attempt in range(3):
        try:
            main()
            break
        except (RuntimeError, ValueError, OSError, subprocess.CalledProcessError, json.JSONDecodeError) as exc:
            if attempt == 2:
                raise
            print(f'Retry {attempt + 1} after error: {exc}')
            time.sleep(2)