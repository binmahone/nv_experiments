#!/usr/bin/env python3
"""
Embed vs Cross-Process Benchmark

Compares two execution modes:
1. EMBED: Python embedded in JVM, no serialization needed
2. CROSS: Separate Python process, Arrow serialization required

All UDFs use proper StructType (no json.dumps)
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

NUM_ROWS = 100000

# ============================================================================
# TEST DATA
# ============================================================================

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

texts = pd.Series([SAMPLE_TEXTS[i % len(SAMPLE_TEXTS)] for i in range(NUM_ROWS)])
nums = pd.Series(np.random.uniform(10, 1000, NUM_ROWS))


# ============================================================================
# 10 REALISTIC UDFs - Return pd.DataFrame (proper StructType)
# ============================================================================

def extract_fields_udf(texts_series: pd.Series) -> pd.DataFrame:
    """5 regex patterns: email, phone, amount, date, order_id"""
    email_pat = re.compile(r'[\w\.-]+@[\w\.-]+\.\w+')
    phone_pat = re.compile(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b')
    amount_pat = re.compile(r'\$[\d,]+(?:\.\d{2})?')
    date_pat = re.compile(r'\b\d{1,2}/\d{1,2}/\d{2,4}\b')
    order_pat = re.compile(r'#(\d{5,})')
    
    emails, phones, amounts, dates, orders, has_contacts = [], [], [], [], [], []
    
    for text in texts_series:
        email = email_pat.search(text)
        phone = phone_pat.search(text)
        amount = amount_pat.search(text)
        date = date_pat.search(text)
        order = order_pat.search(text)
        
        emails.append(email.group() if email else None)
        phones.append(phone.group() if phone else None)
        amounts.append(amount.group() if amount else None)
        dates.append(date.group() if date else None)
        orders.append(order.group(1) if order else None)
        has_contacts.append(email is not None or phone is not None)
    
    return pd.DataFrame({
        'email': emails, 'phone': phones, 'amount': amounts,
        'date': dates, 'order_id': orders, 'has_contact': has_contacts
    })


def normalize_text_udf(texts_series: pd.Series) -> pd.DataFrame:
    """Text cleaning: lowercase, remove punctuation, collapse whitespace"""
    punct_pat = re.compile(r'[^\w\s]')
    space_pat = re.compile(r'\s+')
    
    normalized, word_counts, char_counts = [], [], []
    
    for text in texts_series:
        cleaned = punct_pat.sub('', text.lower())
        cleaned = space_pat.sub(' ', cleaned).strip()
        normalized.append(cleaned)
        word_counts.append(len(cleaned.split()))
        char_counts.append(len(cleaned))
    
    return pd.DataFrame({
        'normalized': normalized, 'word_count': word_counts, 'char_count': char_counts
    })


def parse_date_udf(texts_series: pd.Series) -> pd.DataFrame:
    """Date parsing with derived fields"""
    date_pat = re.compile(r'\d{1,2}/\d{1,2}/\d{2,4}')
    
    years, months, days, weekdays, quarters, is_weekends = [], [], [], [], [], []
    
    for i, text in enumerate(texts_series):
        date_match = date_pat.search(text)
        if date_match:
            try:
                dt = datetime.strptime(date_match.group(), '%m/%d/%Y')
            except:
                dt = datetime(2024, 1, 15)
        else:
            dt = datetime(2024, 1, 1) + timedelta(days=i % 365)
        
        years.append(dt.year)
        months.append(dt.month)
        days.append(dt.day)
        weekdays.append(dt.strftime('%A'))
        quarters.append((dt.month - 1) // 3 + 1)
        is_weekends.append(dt.weekday() >= 5)
    
    return pd.DataFrame({
        'year': years, 'month': months, 'day': days,
        'weekday': weekdays, 'quarter': quarters, 'is_weekend': is_weekends
    })


def url_parse_udf(texts_series: pd.Series, nums_series: pd.Series) -> pd.DataFrame:
    """URL parsing and component extraction"""
    url_pat = re.compile(r'https?://[^\s]+')
    
    schemes, domains, paths, query_ids, is_apis = [], [], [], [], []
    
    for i, (text, num) in enumerate(zip(texts_series, nums_series)):
        url_match = url_pat.search(text)
        if url_match:
            url = url_match.group()
        else:
            domain_list = ['example.com', 'test.org', 'shop.io', 'api.dev']
            path_list = ['/home', '/products', '/api/v1/users', '/checkout']
            url = f"https://{domain_list[i % 4]}{path_list[i % 4]}?id={int(num)}"
        
        parsed = urllib.parse.urlparse(url)
        query_params = urllib.parse.parse_qs(parsed.query)
        
        schemes.append(parsed.scheme)
        domains.append(parsed.netloc)
        paths.append(parsed.path)
        query_ids.append(query_params.get('id', [None])[0])
        is_apis.append('/api/' in parsed.path)
    
    return pd.DataFrame({
        'scheme': schemes, 'domain': domains, 'path': paths,
        'query_id': query_ids, 'is_api': is_apis
    })


def validate_format_udf(texts_series: pd.Series) -> pd.DataFrame:
    """Data validation with Luhn algorithm"""
    email_pat = re.compile(r'^[\w\.-]+@[\w\.-]+\.\w{2,}$')
    
    def luhn_check(num_str):
        digits = [int(d) for d in num_str if d.isdigit()]
        if len(digits) < 13:
            return False
        checksum = 0
        for i, d in enumerate(reversed(digits)):
            if i % 2 == 1:
                d = d * 2
                if d > 9:
                    d -= 9
            checksum += d
        return checksum % 10 == 0
    
    has_emails, valid_emails, cc_valids, length_oks = [], [], [], []
    
    for text in texts_series:
        email_match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', text)
        valid_email = bool(email_pat.match(email_match.group())) if email_match else False
        cc_match = re.search(r'\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}', text)
        cc_valid = luhn_check(cc_match.group()) if cc_match else False
        
        has_emails.append('@' in text)
        valid_emails.append(valid_email)
        cc_valids.append(cc_valid)
        length_oks.append(10 <= len(text) <= 500)
    
    return pd.DataFrame({
        'has_email': has_emails, 'valid_email': valid_emails,
        'cc_valid': cc_valids, 'length_ok': length_oks
    })


def encode_decode_udf(texts_series: pd.Series) -> pd.DataFrame:
    """Base64, URL encode, MD5, SHA256"""
    b64s, url_encodeds, md5s, sigs = [], [], [], []
    
    for text in texts_series:
        text_bytes = text.encode('utf-8')
        b64s.append(base64.b64encode(text_bytes).decode('ascii')[:50])
        url_encodeds.append(urllib.parse.quote(text)[:50])
        md5s.append(hashlib.md5(text_bytes).hexdigest())
        sigs.append(hashlib.sha256(text_bytes).hexdigest()[:32])
    
    return pd.DataFrame({
        'b64': b64s, 'url_encoded': url_encodeds, 'md5': md5s, 'sha256_sig': sigs
    })


def business_rules_udf(texts_series: pd.Series, nums_series: pd.Series) -> pd.DataFrame:
    """Complex pricing and risk rules"""
    tiers, discounts, finals, risks, approveds = [], [], [], [], []
    
    for text, num in zip(texts_series, nums_series):
        tier = 'gold' if num > 500 else 'silver'
        
        discount = 0
        if 'new' in text.lower() or 'first' in text.lower():
            discount += 10
        if num > 800:
            discount += 15
        if 'promo' in text.lower():
            discount += 5
        discount = min(discount, 25)
        
        final_price = round(num * (1 - discount / 100), 2)
        
        risk = 0
        if num > 900:
            risk += 2
        if len(text) < 20:
            risk += 1
        if '@' not in text:
            risk += 1
        
        tiers.append(tier)
        discounts.append(discount)
        finals.append(final_price)
        risks.append(risk)
        approveds.append(risk < 3)
    
    return pd.DataFrame({
        'tier': tiers, 'discount_pct': discounts, 'final_price': finals,
        'risk_score': risks, 'approved': approveds
    })


def text_transform_udf(texts_series: pd.Series) -> pd.DataFrame:
    """String transformations: slug, title case, preview"""
    titles, slugs, previews, initials_list = [], [], [], []
    
    for text in texts_series:
        title = text.title()[:50]
        slug = re.sub(r'[^\w\s-]', '', text.lower())
        slug = re.sub(r'[\s_]+', '-', slug).strip('-')[:50]
        preview = text[:30] + '...' if len(text) > 30 else text
        words = text.split()
        initials = ''.join(w[0].upper() for w in words[:3] if w)
        
        titles.append(title)
        slugs.append(slug)
        previews.append(preview)
        initials_list.append(initials)
    
    return pd.DataFrame({
        'title_case': titles, 'slug': slugs, 'preview': previews, 'initials': initials_list
    })


def data_mask_udf(texts_series: pd.Series) -> pd.DataFrame:
    """PII masking for GDPR compliance"""
    email_pat = re.compile(r'([\w\.-]+)@([\w\.-]+\.\w+)')
    phone_pat = re.compile(r'\b(\d{3})[-.]?(\d{3})[-.]?(\d{4})\b')
    cc_pat = re.compile(r'\b(\d{4})[-\s]?(\d{4})[-\s]?(\d{4})[-\s]?(\d{4})\b')
    ssn_pat = re.compile(r'\b(\d{3})[-]?(\d{2})[-]?(\d{4})\b')
    
    masked_texts, pii_counts = [], []
    
    for text in texts_series:
        masked = text
        count = 0
        
        if email_pat.search(masked):
            masked = email_pat.sub(r'\1[...]@\2', masked)
            count += 1
        if phone_pat.search(masked):
            masked = phone_pat.sub(r'***-***-\3', masked)
            count += 1
        if cc_pat.search(masked):
            masked = cc_pat.sub(r'****-****-****-\4', masked)
            count += 1
        if ssn_pat.search(masked):
            masked = ssn_pat.sub(r'***-**-\3', masked)
            count += 1
        
        masked_texts.append(masked)
        pii_counts.append(count)
    
    return pd.DataFrame({'masked_text': masked_texts, 'pii_count': pii_counts})


def parse_json_udf(texts_series: pd.Series, nums_series: pd.Series) -> pd.DataFrame:
    """JSON parsing and flattening"""
    user_names, user_ids, event_types, sources, is_clicks = [], [], [], [], []
    
    for text, num in zip(texts_series, nums_series):
        user_names.append(text[:20].strip())
        user_ids.append(int(num))
        event_type = 'click' if num > 500 else 'view'
        event_types.append(event_type)
        sources.append('web' if '@' in text else 'mobile')
        is_clicks.append(event_type == 'click')
    
    return pd.DataFrame({
        'user_name': user_names, 'user_id': user_ids, 'event_type': event_types,
        'source': sources, 'is_click': is_clicks
    })


# ============================================================================
# ARROW SCHEMAS
# ============================================================================

SCHEMAS = {
    'extract_fields': pa.schema([
        ('email', pa.string()), ('phone', pa.string()), ('amount', pa.string()),
        ('date', pa.string()), ('order_id', pa.string()), ('has_contact', pa.bool_())
    ]),
    'normalize_text': pa.schema([
        ('normalized', pa.string()), ('word_count', pa.int64()), ('char_count', pa.int64())
    ]),
    'parse_date': pa.schema([
        ('year', pa.int64()), ('month', pa.int64()), ('day', pa.int64()),
        ('weekday', pa.string()), ('quarter', pa.int64()), ('is_weekend', pa.bool_())
    ]),
    'url_parse': pa.schema([
        ('scheme', pa.string()), ('domain', pa.string()), ('path', pa.string()),
        ('query_id', pa.string()), ('is_api', pa.bool_())
    ]),
    'validate_format': pa.schema([
        ('has_email', pa.bool_()), ('valid_email', pa.bool_()),
        ('cc_valid', pa.bool_()), ('length_ok', pa.bool_())
    ]),
    'encode_decode': pa.schema([
        ('b64', pa.string()), ('url_encoded', pa.string()),
        ('md5', pa.string()), ('sha256_sig', pa.string())
    ]),
    'business_rules': pa.schema([
        ('tier', pa.string()), ('discount_pct', pa.int64()), ('final_price', pa.float64()),
        ('risk_score', pa.int64()), ('approved', pa.bool_())
    ]),
    'text_transform': pa.schema([
        ('title_case', pa.string()), ('slug', pa.string()),
        ('preview', pa.string()), ('initials', pa.string())
    ]),
    'data_mask': pa.schema([
        ('masked_text', pa.string()), ('pii_count', pa.int64())
    ]),
    'parse_json': pa.schema([
        ('user_name', pa.string()), ('user_id', pa.int64()), ('event_type', pa.string()),
        ('source', pa.string()), ('is_click', pa.bool_())
    ])
}


# ============================================================================
# BENCHMARK FUNCTIONS
# ============================================================================

def simulate_cross_process(func, inputs, schema):
    """
    Simulate cross-process mode:
    1. Serialize input (Arrow)
    2. Execute UDF
    3. Serialize output (Arrow)
    """
    # Input serialization (JVM -> Python)
    input_start = time.perf_counter()
    input_table = pa.table({'texts': pa.array(inputs[0].tolist(), type=pa.utf8())})
    if len(inputs) > 1:
        input_table = input_table.append_column('nums', pa.array(inputs[1].tolist(), type=pa.float64()))
    sink = io.BytesIO()
    with ipc.new_stream(sink, input_table.schema) as w:
        w.write_table(input_table)
    input_bytes = sink.getvalue()
    
    # Deserialize input
    reader = ipc.open_stream(io.BytesIO(input_bytes))
    _ = reader.read_all()
    input_ser_time = (time.perf_counter() - input_start) * 1000
    
    # UDF execution
    udf_start = time.perf_counter()
    result_df = func(*inputs)
    udf_time = (time.perf_counter() - udf_start) * 1000
    
    # Output serialization (Python -> JVM)
    output_start = time.perf_counter()
    output_table = pa.Table.from_pandas(result_df, schema=schema)
    sink = io.BytesIO()
    with ipc.new_stream(sink, output_table.schema) as w:
        w.write_table(output_table)
    output_bytes = sink.getvalue()
    
    # Deserialize output
    reader = ipc.open_stream(io.BytesIO(output_bytes))
    _ = reader.read_all()
    output_ser_time = (time.perf_counter() - output_start) * 1000
    
    return udf_time, input_ser_time, output_ser_time, len(input_bytes), len(output_bytes)


def simulate_embed_mode(func, inputs):
    """
    Simulate embed mode:
    - No serialization, direct memory access
    - UDF execution only
    """
    udf_start = time.perf_counter()
    result_df = func(*inputs)
    udf_time = (time.perf_counter() - udf_start) * 1000
    
    # Simulate zero-copy access (negligible overhead)
    _ = result_df
    
    return udf_time


# ============================================================================
# MAIN BENCHMARK
# ============================================================================

UDFS = [
    ("extract_fields", "5 regex patterns", extract_fields_udf, [texts]),
    ("normalize_text", "text cleaning", normalize_text_udf, [texts]),
    ("parse_date", "date parsing", parse_date_udf, [texts]),
    ("url_parse", "URL extraction", url_parse_udf, [texts, nums]),
    ("validate_format", "Luhn algorithm", validate_format_udf, [texts]),
    ("encode_decode", "hash + encode", encode_decode_udf, [texts]),
    ("business_rules", "pricing rules", business_rules_udf, [texts, nums]),
    ("text_transform", "slug/title", text_transform_udf, [texts]),
    ("data_mask", "PII masking", data_mask_udf, [texts]),
    ("parse_json", "JSON flatten", parse_json_udf, [texts, nums]),
]


# Warmup
print("Warming up...")
for _ in range(3):
    _ = extract_fields_udf(texts[:1000])
    df = pd.DataFrame({'a': range(1000)})
    table = pa.Table.from_pandas(df)
    sink = io.BytesIO()
    with ipc.new_stream(sink, table.schema) as w:
        w.write_table(table)
print("Done.\n")


print("=" * 120)
print("EMBED vs CROSS-PROCESS Benchmark")
print("=" * 120)
print(f"\nRows: {NUM_ROWS:,} | All UDFs use proper StructType (no json.dumps)")
print()

print(f"{'─'*120}")
print(f"{'UDF':<16} {'Description':<18} {'EMBED':>10} {'CROSS':>10} {'Speedup':>10} "
      f"{'Input Ser':>10} {'Output Ser':>12} {'Total Ser':>10}")
print(f"{'─'*120}")

total_embed = 0
total_cross = 0
total_input_ser = 0
total_output_ser = 0

results = []

for name, desc, func, inputs in UDFS:
    schema = SCHEMAS[name]
    
    # Run embed mode
    embed_time = simulate_embed_mode(func, inputs)
    
    # Run cross-process mode
    udf_time, input_ser, output_ser, input_size, output_size = simulate_cross_process(func, inputs, schema)
    cross_time = udf_time + input_ser + output_ser
    
    speedup = cross_time / embed_time if embed_time > 0 else 0
    
    total_embed += embed_time
    total_cross += cross_time
    total_input_ser += input_ser
    total_output_ser += output_ser
    
    results.append({
        'name': name,
        'embed': embed_time,
        'cross': cross_time,
        'speedup': speedup,
        'input_ser': input_ser,
        'output_ser': output_ser
    })
    
    print(f"{name:<16} {desc:<18} {embed_time:>8.0f}ms {cross_time:>8.0f}ms {speedup:>9.2f}x "
          f"{input_ser:>8.0f}ms {output_ser:>10.0f}ms {input_ser+output_ser:>8.0f}ms")

print(f"{'─'*120}")
overall_speedup = total_cross / total_embed if total_embed > 0 else 0
print(f"{'TOTAL':<16} {'':<18} {total_embed:>8.0f}ms {total_cross:>8.0f}ms {overall_speedup:>9.2f}x "
      f"{total_input_ser:>8.0f}ms {total_output_ser:>10.0f}ms {total_input_ser+total_output_ser:>8.0f}ms")
print(f"{'─'*120}")


# Summary
print(f"""
================================================================================
SUMMARY
================================================================================

Mode Comparison:
  EMBED (no serialization):  {total_embed:,.0f}ms
  CROSS (with Arrow ser):    {total_cross:,.0f}ms
  
  EMBED is {overall_speedup:.2f}x faster than CROSS

Serialization Breakdown:
  Input serialization:       {total_input_ser:,.0f}ms ({total_input_ser/total_cross*100:.1f}% of CROSS)
  Output serialization:      {total_output_ser:,.0f}ms ({total_output_ser/total_cross*100:.1f}% of CROSS)
  Total serialization:       {total_input_ser+total_output_ser:,.0f}ms ({(total_input_ser+total_output_ser)/total_cross*100:.1f}% of CROSS)
  
  UDF computation:           {total_embed:,.0f}ms ({total_embed/total_cross*100:.1f}% of CROSS)

Key Insight:
  For compute-intensive UDFs, serialization overhead is small ({(total_input_ser+total_output_ser)/total_cross*100:.1f}%)
  EMBED mode eliminates this overhead entirely, giving {overall_speedup:.2f}x speedup
""")


# Per-UDF analysis
print("=" * 120)
print("Per-UDF Analysis (sorted by speedup)")
print("=" * 120)

sorted_results = sorted(results, key=lambda x: x['speedup'], reverse=True)

print(f"\n{'UDF':<20} {'EMBED':>10} {'Serialization':>15} {'Speedup':>10} {'Analysis'}")
print(f"{'─'*80}")

for r in sorted_results:
    ser_time = r['input_ser'] + r['output_ser']
    ser_pct = ser_time / r['cross'] * 100 if r['cross'] > 0 else 0
    
    if ser_pct > 30:
        analysis = "High ser overhead - EMBED helps a lot"
    elif ser_pct > 15:
        analysis = "Moderate overhead - EMBED helps"
    else:
        analysis = "Low overhead - compute bound"
    
    print(f"{r['name']:<20} {r['embed']:>8.0f}ms {ser_time:>8.0f}ms ({ser_pct:>4.0f}%) {r['speedup']:>9.2f}x  {analysis}")
