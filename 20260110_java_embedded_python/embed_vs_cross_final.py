#!/usr/bin/env python3
"""
EMBED vs CROSS Benchmark - Final Version
Batch Size: 10,000 (Spark default)
"""

import time
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.ipc as ipc
import io
import re
import hashlib
import base64
import urllib.parse
from datetime import datetime, timedelta

TOTAL_ROWS = 100000
BATCH_SIZE = 10000  # Spark default

np.random.seed(42)

SAMPLE_TEXTS = [
    "John Smith (john.smith@company.com) called 555-123-4567 about order #12345 on 01/15/2024.",
    "Please contact support@example.org for your refund of $150.00 - ticket #98765.",
    "Dr. Sarah Johnson will meet you at 10:30 AM. Her email is sarah.j@hospital.edu.",
    "Credit card ending 4532 was charged $1,299.99 on 12/25/2023 for order #54321.",
    "New customer Jane Doe (jane@gmail.com) placed first order - apply PROMO discount!",
    "API endpoint https://api.example.com/v1/users?id=123 returned 500 error.",
    "Bulk order from enterprise@bigcorp.io: 500 units at $45.00 each = $22,500.00",
    "SSN 123-45-6789 and CC 4111-1111-1111-1111 detected - needs masking!",
]

all_texts = [SAMPLE_TEXTS[i % len(SAMPLE_TEXTS)] for i in range(TOTAL_ROWS)]
all_nums = list(np.random.uniform(10, 1000, TOTAL_ROWS))

# ============================================================================
# 10 UDFs
# ============================================================================

def extract_fields_udf(texts, nums=None):
    email_pat = re.compile(r'[\w\.-]+@[\w\.-]+\.\w+')
    phone_pat = re.compile(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b')
    amount_pat = re.compile(r'\$[\d,]+(?:\.\d{2})?')
    date_pat = re.compile(r'\b\d{1,2}/\d{1,2}/\d{2,4}\b')
    order_pat = re.compile(r'#(\d{5,})')
    results = []
    for text in texts:
        e, p, a, d, o = email_pat.search(text), phone_pat.search(text), amount_pat.search(text), date_pat.search(text), order_pat.search(text)
        results.append({'email': e.group() if e else None, 'phone': p.group() if p else None,
                       'amount': a.group() if a else None, 'date': d.group() if d else None,
                       'order_id': o.group(1) if o else None, 'has_contact': e is not None or p is not None})
    return pd.DataFrame(results)

def normalize_text_udf(texts, nums=None):
    punct_pat, space_pat = re.compile(r'[^\w\s]'), re.compile(r'\s+')
    results = []
    for text in texts:
        cleaned = space_pat.sub(' ', punct_pat.sub('', text.lower())).strip()
        results.append({'normalized': cleaned, 'word_count': len(cleaned.split()), 'char_count': len(cleaned)})
    return pd.DataFrame(results)

def parse_date_udf(texts, nums=None):
    date_pat = re.compile(r'\d{1,2}/\d{1,2}/\d{2,4}')
    results = []
    for i, text in enumerate(texts):
        m = date_pat.search(text)
        try: dt = datetime.strptime(m.group(), '%m/%d/%Y') if m else datetime(2024,1,1)+timedelta(days=i%365)
        except: dt = datetime(2024,1,15)
        results.append({'year': dt.year, 'month': dt.month, 'day': dt.day,
                       'weekday': dt.strftime('%A'), 'quarter': (dt.month-1)//3+1, 'is_weekend': dt.weekday()>=5})
    return pd.DataFrame(results)

def url_parse_udf(texts, nums):
    url_pat = re.compile(r'https?://[^\s]+')
    results = []
    for i, (text, num) in enumerate(zip(texts, nums)):
        m = url_pat.search(text)
        url = m.group() if m else f"https://example.com/path?id={int(num)}"
        p = urllib.parse.urlparse(url)
        q = urllib.parse.parse_qs(p.query)
        results.append({'scheme': p.scheme, 'domain': p.netloc, 'path': p.path,
                       'query_id': q.get('id', [None])[0], 'is_api': '/api/' in p.path})
    return pd.DataFrame(results)

def validate_format_udf(texts, nums=None):
    email_pat = re.compile(r'^[\w\.-]+@[\w\.-]+\.\w{2,}$')
    def luhn(s):
        d = [int(c) for c in s if c.isdigit()]
        if len(d) < 13: return False
        return sum([(x*2-9 if x*2>9 else x*2) if i%2==1 else x for i,x in enumerate(reversed(d))]) % 10 == 0
    results = []
    for text in texts:
        em = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', text)
        cc = re.search(r'\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}', text)
        results.append({'has_email': '@' in text, 'valid_email': bool(email_pat.match(em.group())) if em else False,
                       'cc_valid': luhn(cc.group()) if cc else False, 'length_ok': 10 <= len(text) <= 500})
    return pd.DataFrame(results)

def encode_decode_udf(texts, nums=None):
    results = []
    for text in texts:
        b = text.encode('utf-8')
        results.append({'b64': base64.b64encode(b).decode('ascii')[:50], 'url_encoded': urllib.parse.quote(text)[:50],
                       'md5': hashlib.md5(b).hexdigest(), 'sha256_sig': hashlib.sha256(b).hexdigest()[:32]})
    return pd.DataFrame(results)

def business_rules_udf(texts, nums):
    results = []
    for text, num in zip(texts, nums):
        discount = min(sum([10 if 'new' in text.lower() or 'first' in text.lower() else 0,
                           15 if num > 800 else 0, 5 if 'promo' in text.lower() else 0]), 25)
        risk = sum([num > 900, len(text) < 20, '@' not in text])
        results.append({'tier': 'gold' if num > 500 else 'silver', 'discount_pct': discount,
                       'final_price': round(num * (1 - discount/100), 2), 'risk_score': risk, 'approved': risk < 3})
    return pd.DataFrame(results)

def text_transform_udf(texts, nums=None):
    results = []
    for text in texts:
        slug = re.sub(r'[\s_]+', '-', re.sub(r'[^\w\s-]', '', text.lower())).strip('-')[:50]
        results.append({'title_case': text.title()[:50], 'slug': slug,
                       'preview': text[:30]+'...' if len(text)>30 else text,
                       'initials': ''.join(w[0].upper() for w in text.split()[:3] if w)})
    return pd.DataFrame(results)

def data_mask_udf(texts, nums=None):
    pats = [(re.compile(r'([\w\.-]+)@([\w\.-]+\.\w+)'), r'\1[...]@\2'),
            (re.compile(r'\b(\d{3})[-.]?(\d{3})[-.]?(\d{4})\b'), r'***-***-\3'),
            (re.compile(r'\b(\d{4})[-\s]?(\d{4})[-\s]?(\d{4})[-\s]?(\d{4})\b'), r'****-****-****-\4'),
            (re.compile(r'\b(\d{3})[-]?(\d{2})[-]?(\d{4})\b'), r'***-**-\3')]
    results = []
    for text in texts:
        masked, cnt = text, 0
        for pat, repl in pats:
            if pat.search(masked): masked, cnt = pat.sub(repl, masked), cnt + 1
        results.append({'masked_text': masked, 'pii_count': cnt})
    return pd.DataFrame(results)

def parse_json_udf(texts, nums):
    results = []
    for text, num in zip(texts, nums):
        evt = 'click' if num > 500 else 'view'
        results.append({'user_name': text[:20].strip(), 'user_id': int(num), 'event_type': evt,
                       'source': 'web' if '@' in text else 'mobile', 'is_click': evt == 'click'})
    return pd.DataFrame(results)

SCHEMAS = {
    'extract_fields': pa.schema([('email', pa.string()), ('phone', pa.string()), ('amount', pa.string()),
                                 ('date', pa.string()), ('order_id', pa.string()), ('has_contact', pa.bool_())]),
    'normalize_text': pa.schema([('normalized', pa.string()), ('word_count', pa.int64()), ('char_count', pa.int64())]),
    'parse_date': pa.schema([('year', pa.int64()), ('month', pa.int64()), ('day', pa.int64()),
                            ('weekday', pa.string()), ('quarter', pa.int64()), ('is_weekend', pa.bool_())]),
    'url_parse': pa.schema([('scheme', pa.string()), ('domain', pa.string()), ('path', pa.string()),
                           ('query_id', pa.string()), ('is_api', pa.bool_())]),
    'validate_format': pa.schema([('has_email', pa.bool_()), ('valid_email', pa.bool_()),
                                  ('cc_valid', pa.bool_()), ('length_ok', pa.bool_())]),
    'encode_decode': pa.schema([('b64', pa.string()), ('url_encoded', pa.string()),
                               ('md5', pa.string()), ('sha256_sig', pa.string())]),
    'business_rules': pa.schema([('tier', pa.string()), ('discount_pct', pa.int64()), ('final_price', pa.float64()),
                                ('risk_score', pa.int64()), ('approved', pa.bool_())]),
    'text_transform': pa.schema([('title_case', pa.string()), ('slug', pa.string()),
                                ('preview', pa.string()), ('initials', pa.string())]),
    'data_mask': pa.schema([('masked_text', pa.string()), ('pii_count', pa.int64())]),
    'parse_json': pa.schema([('user_name', pa.string()), ('user_id', pa.int64()), ('event_type', pa.string()),
                            ('source', pa.string()), ('is_click', pa.bool_())])
}

UDFS = [
    ("extract_fields", "5 regex", extract_fields_udf, False),
    ("normalize_text", "text clean", normalize_text_udf, False),
    ("parse_date", "date parse", parse_date_udf, False),
    ("url_parse", "URL extract", url_parse_udf, True),
    ("validate_format", "Luhn check", validate_format_udf, False),
    ("encode_decode", "hash/encode", encode_decode_udf, False),
    ("business_rules", "pricing", business_rules_udf, True),
    ("text_transform", "slug/title", text_transform_udf, False),
    ("data_mask", "PII mask", data_mask_udf, False),
    ("parse_json", "JSON flat", parse_json_udf, True),
]

def run_benchmark(func, has_nums, schema):
    num_batches = TOTAL_ROWS // BATCH_SIZE
    embed_time, cross_udf_time, cross_ser_time = 0, 0, 0
    
    for i in range(num_batches):
        s, e = i * BATCH_SIZE, (i + 1) * BATCH_SIZE
        batch_texts, batch_nums = all_texts[s:e], all_nums[s:e]
        
        # EMBED
        t0 = time.perf_counter()
        _ = func(batch_texts, batch_nums) if has_nums else func(batch_texts)
        embed_time += (time.perf_counter() - t0) * 1000
        
        # CROSS - input ser
        t0 = time.perf_counter()
        tbl = pa.table({'texts': pa.array(batch_texts, type=pa.utf8())})
        if has_nums: tbl = tbl.append_column('nums', pa.array(batch_nums, type=pa.float64()))
        sink = io.BytesIO()
        with ipc.new_stream(sink, tbl.schema) as w: w.write_table(tbl)
        _ = ipc.open_stream(io.BytesIO(sink.getvalue())).read_all()
        input_ser = (time.perf_counter() - t0) * 1000
        
        # CROSS - udf
        t0 = time.perf_counter()
        result = func(batch_texts, batch_nums) if has_nums else func(batch_texts)
        udf_t = (time.perf_counter() - t0) * 1000
        
        # CROSS - output ser
        t0 = time.perf_counter()
        out_tbl = pa.Table.from_pandas(result, schema=schema)
        sink = io.BytesIO()
        with ipc.new_stream(sink, out_tbl.schema) as w: w.write_table(out_tbl)
        out_bytes = sink.getvalue()
        _ = ipc.open_stream(io.BytesIO(out_bytes)).read_all()
        output_ser = (time.perf_counter() - t0) * 1000
        
        cross_udf_time += udf_t
        cross_ser_time += input_ser + output_ser
    
    return embed_time, cross_udf_time, cross_ser_time, len(out_bytes) * num_batches

# Warmup
print("Warming up...")
for _ in range(3): _ = extract_fields_udf(all_texts[:1000])
print()

print("=" * 110)
print(f"EMBED vs CROSS-PROCESS | Rows: {TOTAL_ROWS:,} | Batch: {BATCH_SIZE:,} | Batches: {TOTAL_ROWS//BATCH_SIZE}")
print("=" * 110)
print(f"{'UDF':<16} {'Type':<12} {'EMBED':>10} {'CROSS':>10} {'UDF':>10} {'Ser':>10} {'Ser%':>8} {'Speedup':>10}")
print("─" * 110)

total_embed, total_cross, total_ser = 0, 0, 0
rows = []

for name, desc, func, has_nums in UDFS:
    embed, udf, ser, size = run_benchmark(func, has_nums, SCHEMAS[name])
    cross = udf + ser
    pct = ser / cross * 100 if cross > 0 else 0
    spd = cross / embed if embed > 0 else 0
    total_embed += embed
    total_cross += cross
    total_ser += ser
    rows.append((name, desc, embed, cross, udf, ser, pct, spd))
    print(f"{name:<16} {desc:<12} {embed:>8.0f}ms {cross:>8.0f}ms {udf:>8.0f}ms {ser:>8.0f}ms {pct:>7.1f}% {spd:>9.2f}x")

print("─" * 110)
total_pct = total_ser / total_cross * 100 if total_cross > 0 else 0
total_spd = total_cross / total_embed if total_embed > 0 else 0
print(f"{'TOTAL':<16} {'':<12} {total_embed:>8.0f}ms {total_cross:>8.0f}ms {total_cross-total_ser:>8.0f}ms {total_ser:>8.0f}ms {total_pct:>7.1f}% {total_spd:>9.2f}x")
print("=" * 110)

print(f"""
┌─────────────────────────────────────────────────────────────┐
│  SUMMARY                                                    │
├─────────────────────────────────────────────────────────────┤
│  EMBED Mode:     {total_embed:>8,.0f} ms  (no serialization)         │
│  CROSS Mode:     {total_cross:>8,.0f} ms  (with Arrow ser)           │
│  Serialization:  {total_ser:>8,.0f} ms  ({total_pct:.1f}% of CROSS)            │
│                                                             │
│  EMBED is {total_spd:.2f}x faster than CROSS                       │
└─────────────────────────────────────────────────────────────┘
""")
