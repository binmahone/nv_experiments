#!/usr/bin/env python3
"""
Realistic UDF Benchmark v3 - Using Python stdlib, Proper StructType

10 Real-World UDFs that represent actual production usage:
1. extract_fields - 5 regex patterns for email, phone, amount, date, order_id
2. parse_json - Parse and flatten JSON structures
3. normalize_text - Text cleaning and normalization
4. parse_date - Date parsing with derived fields
5. url_parse - URL component extraction
6. validate_format - Data validation with Luhn algorithm
7. encode_decode - Base64, URL encode, hashing
8. business_rules - Complex pricing and risk rules
9. text_transform - String transformations (slug, title case, etc.)
10. data_mask - PII masking (GDPR compliance)
"""

import time
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.ipc as ipc
import io
import re
import json
import hashlib
import base64
import urllib.parse
from datetime import datetime, timedelta

NUM_ROWS = 100000

# ============================================================================
# REALISTIC TEST DATA
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

texts = [SAMPLE_TEXTS[i % len(SAMPLE_TEXTS)] for i in range(NUM_ROWS)]
nums = np.random.uniform(10, 1000, NUM_ROWS)


# ============================================================================
# 10 REALISTIC UDFs - Proper StructType Returns
# ============================================================================

def extract_fields_udf(texts_series: pd.Series) -> pd.DataFrame:
    """
    UDF 1: Extract multiple fields using 5 regex patterns.
    Real scenario: Parse logs, extract info from emails/documents.
    
    Returns: struct<email, phone, amount, date, order_id, has_contact>
    """
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
        'email': emails,
        'phone': phones,
        'amount': amounts,
        'date': dates,
        'order_id': orders,
        'has_contact': has_contacts
    })


def parse_json_udf(texts_series: pd.Series, nums_series: pd.Series) -> pd.DataFrame:
    """
    UDF 2: Parse JSON and flatten nested structures.
    Real scenario: Process API responses, event payloads.
    
    Returns: struct<user_name, user_id, event_type, source, is_click>
    """
    user_names, user_ids, event_types, sources, is_clicks = [], [], [], [], []
    
    for text, num in zip(texts_series, nums_series):
        # Simulate JSON payload parsing
        user_names.append(text[:20].strip())
        user_ids.append(int(num))
        event_type = 'click' if num > 500 else 'view'
        event_types.append(event_type)
        sources.append('web' if '@' in text else 'mobile')
        is_clicks.append(event_type == 'click')
    
    return pd.DataFrame({
        'user_name': user_names,
        'user_id': user_ids,
        'event_type': event_types,
        'source': sources,
        'is_click': is_clicks
    })


def normalize_text_udf(texts_series: pd.Series) -> pd.DataFrame:
    """
    UDF 3: Clean and normalize text for indexing/comparison.
    Real scenario: Search preprocessing, deduplication.
    
    Returns: struct<normalized, word_count, char_count>
    """
    punct_pat = re.compile(r'[^\w\s]')
    space_pat = re.compile(r'\s+')
    
    normalized, word_counts, char_counts = [], [], []
    
    for text in texts_series:
        # Lowercase, remove punctuation, collapse whitespace
        cleaned = text.lower()
        cleaned = punct_pat.sub('', cleaned)
        cleaned = space_pat.sub(' ', cleaned).strip()
        
        normalized.append(cleaned)
        word_counts.append(len(cleaned.split()))
        char_counts.append(len(cleaned))
    
    return pd.DataFrame({
        'normalized': normalized,
        'word_count': word_counts,
        'char_count': char_counts
    })


def parse_date_udf(texts_series: pd.Series) -> pd.DataFrame:
    """
    UDF 4: Parse dates and compute derived fields.
    Real scenario: ETL date normalization from multiple sources.
    
    Returns: struct<year, month, day, weekday, quarter, is_weekend>
    """
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
        'year': years,
        'month': months,
        'day': days,
        'weekday': weekdays,
        'quarter': quarters,
        'is_weekend': is_weekends
    })


def url_parse_udf(texts_series: pd.Series, nums_series: pd.Series) -> pd.DataFrame:
    """
    UDF 5: Parse URLs and extract components.
    Real scenario: Web traffic analysis, referrer parsing.
    
    Returns: struct<scheme, domain, path, query_id, is_api>
    """
    url_pat = re.compile(r'https?://[^\s]+')
    
    schemes, domains, paths, query_ids, is_apis = [], [], [], [], []
    
    for i, (text, num) in enumerate(zip(texts_series, nums_series)):
        url_match = url_pat.search(text)
        if url_match:
            url = url_match.group()
        else:
            # Construct a URL
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
        'scheme': schemes,
        'domain': domains,
        'path': paths,
        'query_id': query_ids,
        'is_api': is_apis
    })


def validate_format_udf(texts_series: pd.Series, nums_series: pd.Series) -> pd.DataFrame:
    """
    UDF 6: Validate data formats with Luhn algorithm for credit cards.
    Real scenario: Data quality checks, input validation.
    
    Returns: struct<has_email, valid_email, cc_valid, length_ok>
    """
    email_pat = re.compile(r'^[\w\.-]+@[\w\.-]+\.\w{2,}$')
    
    def luhn_check(num_str):
        """Luhn algorithm for credit card validation"""
        digits = [int(d) for d in num_str if d.isdigit()]
        if len(digits) < 13:
            return False
        checksum = 0
        for i, d in enumerate(reversed(digits)):
            if i % 2 == 1:
                d *= 2
                if d > 9:
                    d -= 9
            checksum += d
        return checksum % 10 == 0
    
    has_emails, valid_emails, cc_valids, length_oks = [], [], [], []
    
    for text, num in zip(texts_series, nums_series):
        has_email = '@' in text
        
        # Extract email and validate format
        email_match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', text)
        valid_email = bool(email_pat.match(email_match.group())) if email_match else False
        
        # Check credit card with Luhn
        cc_match = re.search(r'\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}', text)
        cc_valid = luhn_check(cc_match.group()) if cc_match else False
        
        has_emails.append(has_email)
        valid_emails.append(valid_email)
        cc_valids.append(cc_valid)
        length_oks.append(10 <= len(text) <= 500)
    
    return pd.DataFrame({
        'has_email': has_emails,
        'valid_email': valid_emails,
        'cc_valid': cc_valids,
        'length_ok': length_oks
    })


def encode_decode_udf(texts_series: pd.Series) -> pd.DataFrame:
    """
    UDF 7: Base64, URL encode, and hash generation.
    Real scenario: Token generation, API encoding, checksums.
    
    Returns: struct<b64, url_encoded, md5, sha256_sig>
    """
    b64s, url_encodeds, md5s, sigs = [], [], [], []
    
    for text in texts_series:
        text_bytes = text.encode('utf-8')
        
        # Base64 encode (truncated for display)
        b64 = base64.b64encode(text_bytes).decode('ascii')[:50]
        
        # URL encode (truncated)
        url_encoded = urllib.parse.quote(text)[:50]
        
        # MD5 hash
        md5 = hashlib.md5(text_bytes).hexdigest()
        
        # SHA256 signature
        sig = hashlib.sha256(text_bytes).hexdigest()[:32]
        
        b64s.append(b64)
        url_encodeds.append(url_encoded)
        md5s.append(md5)
        sigs.append(sig)
    
    return pd.DataFrame({
        'b64': b64s,
        'url_encoded': url_encodeds,
        'md5': md5s,
        'sha256_sig': sigs
    })


def business_rules_udf(texts_series: pd.Series, nums_series: pd.Series) -> pd.DataFrame:
    """
    UDF 8: Complex business rule evaluation.
    Real scenario: Dynamic pricing, eligibility checks, fraud scoring.
    
    Returns: struct<tier, discount_pct, final_price, risk_score, approved>
    """
    tiers, discounts, finals, risks, approveds = [], [], [], [], []
    
    for text, num in zip(texts_series, nums_series):
        # Tier determination
        is_premium = num > 500
        tier = 'gold' if is_premium else 'silver'
        
        # Discount calculation (multiple rules)
        discount = 0
        if 'new' in text.lower() or 'first' in text.lower():
            discount += 10
        if num > 800:  # Bulk order
            discount += 15
        if 'promo' in text.lower() or 'discount' in text.lower():
            discount += 5
        discount = min(discount, 25)  # Cap at 25%
        
        # Final price
        final_price = round(num * (1 - discount / 100), 2)
        
        # Risk scoring
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
        'tier': tiers,
        'discount_pct': discounts,
        'final_price': finals,
        'risk_score': risks,
        'approved': approveds
    })


def text_transform_udf(texts_series: pd.Series) -> pd.DataFrame:
    """
    UDF 9: String transformations for display and URLs.
    Real scenario: Generate slugs, display names, preview text.
    
    Returns: struct<title_case, slug, preview, initials>
    """
    titles, slugs, previews, initials_list = [], [], [], []
    
    for text in texts_series:
        # Title case
        title = text.title()[:50]
        
        # URL slug
        slug = re.sub(r'[^\w\s-]', '', text.lower())
        slug = re.sub(r'[\s_]+', '-', slug).strip('-')[:50]
        
        # Preview with ellipsis
        preview = text[:30] + '...' if len(text) > 30 else text
        
        # Initials (first 3 words)
        words = text.split()
        initials = ''.join(w[0].upper() for w in words[:3] if w)
        
        titles.append(title)
        slugs.append(slug)
        previews.append(preview)
        initials_list.append(initials)
    
    return pd.DataFrame({
        'title_case': titles,
        'slug': slugs,
        'preview': previews,
        'initials': initials_list
    })


def data_mask_udf(texts_series: pd.Series) -> pd.DataFrame:
    """
    UDF 10: PII data masking for compliance.
    Real scenario: GDPR, log sanitization, data anonymization.
    
    Returns: struct<masked_text, pii_count>
    """
    email_pat = re.compile(r'([\w\.-]+)@([\w\.-]+\.\w+)')
    phone_pat = re.compile(r'\b(\d{3})[-.]?(\d{3})[-.]?(\d{4})\b')
    cc_pat = re.compile(r'\b(\d{4})[-\s]?(\d{4})[-\s]?(\d{4})[-\s]?(\d{4})\b')
    ssn_pat = re.compile(r'\b(\d{3})[-]?(\d{2})[-]?(\d{4})\b')
    
    masked_texts, pii_counts = [], []
    
    for text in texts_series:
        masked = text
        count = 0
        
        # Mask email: keep first char and domain
        if email_pat.search(masked):
            masked = email_pat.sub(r'\1[...]@\2', masked)
            count += 1
        
        # Mask phone: keep last 4
        if phone_pat.search(masked):
            masked = phone_pat.sub(r'***-***-\3', masked)
            count += 1
        
        # Mask credit card: keep last 4
        if cc_pat.search(masked):
            masked = cc_pat.sub(r'****-****-****-\4', masked)
            count += 1
        
        # Mask SSN: keep last 4
        if ssn_pat.search(masked):
            masked = ssn_pat.sub(r'***-**-\3', masked)
            count += 1
        
        masked_texts.append(masked)
        pii_counts.append(count)
    
    return pd.DataFrame({
        'masked_text': masked_texts,
        'pii_count': pii_counts
    })


# ============================================================================
# ARROW SCHEMAS
# ============================================================================

SCHEMAS = {
    'extract_fields': pa.schema([
        ('email', pa.string()),
        ('phone', pa.string()),
        ('amount', pa.string()),
        ('date', pa.string()),
        ('order_id', pa.string()),
        ('has_contact', pa.bool_())
    ]),
    'parse_json': pa.schema([
        ('user_name', pa.string()),
        ('user_id', pa.int64()),
        ('event_type', pa.string()),
        ('source', pa.string()),
        ('is_click', pa.bool_())
    ]),
    'normalize_text': pa.schema([
        ('normalized', pa.string()),
        ('word_count', pa.int64()),
        ('char_count', pa.int64())
    ]),
    'parse_date': pa.schema([
        ('year', pa.int64()),
        ('month', pa.int64()),
        ('day', pa.int64()),
        ('weekday', pa.string()),
        ('quarter', pa.int64()),
        ('is_weekend', pa.bool_())
    ]),
    'url_parse': pa.schema([
        ('scheme', pa.string()),
        ('domain', pa.string()),
        ('path', pa.string()),
        ('query_id', pa.string()),
        ('is_api', pa.bool_())
    ]),
    'validate_format': pa.schema([
        ('has_email', pa.bool_()),
        ('valid_email', pa.bool_()),
        ('cc_valid', pa.bool_()),
        ('length_ok', pa.bool_())
    ]),
    'encode_decode': pa.schema([
        ('b64', pa.string()),
        ('url_encoded', pa.string()),
        ('md5', pa.string()),
        ('sha256_sig', pa.string())
    ]),
    'business_rules': pa.schema([
        ('tier', pa.string()),
        ('discount_pct', pa.int64()),
        ('final_price', pa.float64()),
        ('risk_score', pa.int64()),
        ('approved', pa.bool_())
    ]),
    'text_transform': pa.schema([
        ('title_case', pa.string()),
        ('slug', pa.string()),
        ('preview', pa.string()),
        ('initials', pa.string())
    ]),
    'data_mask': pa.schema([
        ('masked_text', pa.string()),
        ('pii_count', pa.int64())
    ])
}


# ============================================================================
# BENCHMARK
# ============================================================================

UDFS = [
    ("extract_fields", "5 regex", extract_fields_udf, [pd.Series(texts)]),
    ("parse_json", "JSON flatten", parse_json_udf, [pd.Series(texts), pd.Series(nums)]),
    ("normalize_text", "text clean", normalize_text_udf, [pd.Series(texts)]),
    ("parse_date", "date parse", parse_date_udf, [pd.Series(texts)]),
    ("url_parse", "URL extract", url_parse_udf, [pd.Series(texts), pd.Series(nums)]),
    ("validate_format", "Luhn check", validate_format_udf, [pd.Series(texts), pd.Series(nums)]),
    ("encode_decode", "hash/encode", encode_decode_udf, [pd.Series(texts)]),
    ("business_rules", "pricing", business_rules_udf, [pd.Series(texts), pd.Series(nums)]),
    ("text_transform", "slug/title", text_transform_udf, [pd.Series(texts)]),
    ("data_mask", "PII mask", data_mask_udf, [pd.Series(texts)]),
]


def measure_arrow_serialization(df: pd.DataFrame, schema: pa.Schema):
    """Measure Arrow serialization of structured DataFrame"""
    start = time.perf_counter()
    
    table = pa.Table.from_pandas(df, schema=schema)
    
    sink = io.BytesIO()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    output_bytes = sink.getvalue()
    
    # Deserialize (simulating JVM side)
    reader = ipc.open_stream(io.BytesIO(output_bytes))
    _ = reader.read_all()
    
    elapsed = (time.perf_counter() - start) * 1000
    return elapsed, len(output_bytes)


# Warmup
print("Warming up...")
for _ in range(3):
    df = pd.DataFrame({'a': range(1000), 'b': ['x']*1000})
    table = pa.Table.from_pandas(df)
    sink = io.BytesIO()
    with ipc.new_stream(sink, table.schema) as w:
        w.write_table(table)
print("Done.\n")


print("=" * 110)
print("Realistic UDF Benchmark v3: 10 Real-World UDFs with Proper StructType")
print("=" * 110)
print(f"\nTotal rows: {NUM_ROWS:,}")
print("All UDFs use Python stdlib (no external ML/NLP libs)")
print("All returns are pd.DataFrame with proper schema (no json.dumps)\n")

print(f"{'─'*110}")
print(f"{'UDF':<16} {'Type':<12} {'Cols':>5} {'UDF Time':>10} {'Arrow':>10} {'Total':>10} "
      f"{'Overhead':>10} {'Size':>10}")
print(f"{'─'*110}")

total_udf = 0
total_arrow = 0

for name, udf_type, func, inputs in UDFS:
    schema = SCHEMAS[name]
    
    # Run UDF
    udf_start = time.perf_counter()
    result_df = func(*inputs)
    udf_time = (time.perf_counter() - udf_start) * 1000
    
    # Measure Arrow serialization
    arrow_time, size = measure_arrow_serialization(result_df, schema)
    total_time = udf_time + arrow_time
    
    overhead = (arrow_time / udf_time * 100) if udf_time > 0 else 0
    total_udf += udf_time
    total_arrow += arrow_time
    
    num_cols = len(result_df.columns)
    
    print(f"{name:<16} {udf_type:<12} {num_cols:>5} {udf_time:>8.0f}ms {arrow_time:>8.0f}ms "
          f"{total_time:>8.0f}ms {overhead:>8.0f}% {size/1024/1024:>8.2f}MB")

print(f"{'─'*110}")
overall_overhead = (total_arrow / total_udf * 100) if total_udf > 0 else 0
print(f"{'TOTAL':<16} {'':<12} {'':<5} {total_udf:>8.0f}ms {total_arrow:>8.0f}ms "
      f"{total_udf+total_arrow:>8.0f}ms {overall_overhead:>8.0f}%")
print(f"{'─'*110}")


# Show sample output for each UDF
print("\n")
print("=" * 110)
print("Sample Output (first row of each UDF)")
print("=" * 110)

for name, udf_type, func, inputs in UDFS[:5]:  # Show first 5
    result_df = func(*inputs)
    print(f"\n{name}:")
    print(f"  Input: {texts[0][:60]}...")
    first_row = result_df.iloc[0].to_dict()
    for k, v in first_row.items():
        v_str = str(v)[:50] + '...' if len(str(v)) > 50 else str(v)
        print(f"  {k}: {v_str}")
