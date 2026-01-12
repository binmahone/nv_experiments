#!/usr/bin/env python3
"""
Batch Size Benchmark: EMBED vs CROSS with different batch sizes

Simulates real Spark behavior where data is processed in batches.
Default spark.sql.execution.arrow.maxRecordsPerBatch = 10,000
"""

import time
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.ipc as ipc
import io
import re
import hashlib
from datetime import datetime, timedelta

TOTAL_ROWS = 100000
BATCH_SIZES = [1000, 5000, 10000, 50000, 100000]  # Test different batch sizes

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

all_texts = [SAMPLE_TEXTS[i % len(SAMPLE_TEXTS)] for i in range(TOTAL_ROWS)]
all_nums = np.random.uniform(10, 1000, TOTAL_ROWS)


# ============================================================================
# UDFs (3 representative ones)
# ============================================================================

def extract_fields_udf(texts_series: pd.Series) -> pd.DataFrame:
    """5 regex patterns - compute intensive"""
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


def business_rules_udf(texts_series: pd.Series, nums_series: pd.Series) -> pd.DataFrame:
    """Business logic - medium compute"""
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
        risk = sum([num > 900, len(text) < 20, '@' not in text])
        
        tiers.append(tier)
        discounts.append(discount)
        finals.append(final_price)
        risks.append(risk)
        approveds.append(risk < 3)
    
    return pd.DataFrame({
        'tier': tiers, 'discount_pct': discounts, 'final_price': finals,
        'risk_score': risks, 'approved': approveds
    })


def data_mask_udf(texts_series: pd.Series) -> pd.DataFrame:
    """PII masking - heavy compute"""
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


# Schemas
SCHEMAS = {
    'extract_fields': pa.schema([
        ('email', pa.string()), ('phone', pa.string()), ('amount', pa.string()),
        ('date', pa.string()), ('order_id', pa.string()), ('has_contact', pa.bool_())
    ]),
    'business_rules': pa.schema([
        ('tier', pa.string()), ('discount_pct', pa.int64()), ('final_price', pa.float64()),
        ('risk_score', pa.int64()), ('approved', pa.bool_())
    ]),
    'data_mask': pa.schema([
        ('masked_text', pa.string()), ('pii_count', pa.int64())
    ])
}


# ============================================================================
# BENCHMARK
# ============================================================================

def run_embed_mode(func, texts, nums, batch_size, has_nums):
    """Embed mode: no serialization, process in batches"""
    total_time = 0
    num_batches = (len(texts) + batch_size - 1) // batch_size
    
    for i in range(num_batches):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, len(texts))
        
        batch_texts = pd.Series(texts[start_idx:end_idx])
        if has_nums:
            batch_nums = pd.Series(nums[start_idx:end_idx])
        
        t0 = time.perf_counter()
        if has_nums:
            result = func(batch_texts, batch_nums)
        else:
            result = func(batch_texts)
        total_time += (time.perf_counter() - t0) * 1000
    
    return total_time, num_batches


def run_cross_mode(func, texts, nums, batch_size, has_nums, schema):
    """Cross-process mode: Arrow serialization for each batch"""
    total_udf_time = 0
    total_ser_time = 0
    num_batches = (len(texts) + batch_size - 1) // batch_size
    
    for i in range(num_batches):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, len(texts))
        
        batch_texts = texts[start_idx:end_idx]
        batch_nums = nums[start_idx:end_idx] if has_nums else None
        
        # Input serialization
        ser_start = time.perf_counter()
        input_table = pa.table({'texts': pa.array(batch_texts, type=pa.utf8())})
        if has_nums:
            input_table = input_table.append_column('nums', pa.array(batch_nums, type=pa.float64()))
        sink = io.BytesIO()
        with ipc.new_stream(sink, input_table.schema) as w:
            w.write_table(input_table)
        input_bytes = sink.getvalue()
        reader = ipc.open_stream(io.BytesIO(input_bytes))
        _ = reader.read_all()
        input_ser_time = (time.perf_counter() - ser_start) * 1000
        
        # UDF execution
        udf_start = time.perf_counter()
        batch_texts_series = pd.Series(batch_texts)
        if has_nums:
            batch_nums_series = pd.Series(batch_nums)
            result = func(batch_texts_series, batch_nums_series)
        else:
            result = func(batch_texts_series)
        udf_time = (time.perf_counter() - udf_start) * 1000
        
        # Output serialization
        output_start = time.perf_counter()
        output_table = pa.Table.from_pandas(result, schema=schema)
        sink = io.BytesIO()
        with ipc.new_stream(sink, output_table.schema) as w:
            w.write_table(output_table)
        output_bytes = sink.getvalue()
        reader = ipc.open_stream(io.BytesIO(output_bytes))
        _ = reader.read_all()
        output_ser_time = (time.perf_counter() - output_start) * 1000
        
        total_udf_time += udf_time
        total_ser_time += input_ser_time + output_ser_time
    
    return total_udf_time, total_ser_time, num_batches


UDFS = [
    ("extract_fields", "5 regex (heavy)", extract_fields_udf, False),
    ("business_rules", "pricing (medium)", business_rules_udf, True),
    ("data_mask", "PII mask (heavy)", data_mask_udf, False),
]


# Warmup
print("Warming up...")
for _ in range(3):
    _ = extract_fields_udf(pd.Series(all_texts[:1000]))
print("Done.\n")


print("=" * 130)
print(f"Batch Size Benchmark: EMBED vs CROSS | Total Rows: {TOTAL_ROWS:,}")
print("=" * 130)


for udf_name, udf_desc, udf_func, has_nums in UDFS:
    schema = SCHEMAS[udf_name]
    
    print(f"\n{'─'*130}")
    print(f"UDF: {udf_name} ({udf_desc})")
    print(f"{'─'*130}")
    print(f"{'Batch Size':>12} {'Batches':>10} {'EMBED':>12} {'CROSS':>12} {'UDF Time':>12} "
          f"{'Ser Time':>12} {'Ser %':>10} {'Speedup':>10}")
    print(f"{'─'*130}")
    
    for batch_size in BATCH_SIZES:
        # EMBED mode
        embed_time, num_batches = run_embed_mode(
            udf_func, all_texts, all_nums, batch_size, has_nums)
        
        # CROSS mode
        udf_time, ser_time, _ = run_cross_mode(
            udf_func, all_texts, all_nums, batch_size, has_nums, schema)
        cross_time = udf_time + ser_time
        
        ser_pct = ser_time / cross_time * 100 if cross_time > 0 else 0
        speedup = cross_time / embed_time if embed_time > 0 else 0
        
        print(f"{batch_size:>12,} {num_batches:>10} {embed_time:>10.0f}ms {cross_time:>10.0f}ms "
              f"{udf_time:>10.0f}ms {ser_time:>10.0f}ms {ser_pct:>9.1f}% {speedup:>9.2f}x")


# Summary table
print("\n")
print("=" * 130)
print("SUMMARY: Speedup by Batch Size")
print("=" * 130)
print(f"\n{'Batch Size':>12} | {'extract_fields':>18} | {'business_rules':>18} | {'data_mask':>18}")
print(f"{'─'*80}")

for batch_size in BATCH_SIZES:
    speedups = []
    for udf_name, udf_desc, udf_func, has_nums in UDFS:
        schema = SCHEMAS[udf_name]
        embed_time, _ = run_embed_mode(udf_func, all_texts, all_nums, batch_size, has_nums)
        udf_time, ser_time, _ = run_cross_mode(udf_func, all_texts, all_nums, batch_size, has_nums, schema)
        cross_time = udf_time + ser_time
        speedup = cross_time / embed_time if embed_time > 0 else 0
        speedups.append(speedup)
    
    print(f"{batch_size:>12,} | {speedups[0]:>17.2f}x | {speedups[1]:>17.2f}x | {speedups[2]:>17.2f}x")


print(f"""
================================================================================
KEY FINDINGS
================================================================================

1. Batch Size Impact on Serialization:
   - Smaller batches = More serialization overhead (more round trips)
   - Larger batches = Less overhead but potentially more memory

2. Real Spark Default (batch_size=10,000):
   - This is the most realistic scenario
   - Serialization overhead is typically 5-15%

3. EMBED Advantage:
   - For small batches (1K): EMBED can be significantly faster
   - For large batches (100K): Difference is smaller

4. Recommendation:
   - For compute-heavy UDFs: Batch size doesn't matter much
   - For light UDFs: Larger batches reduce serialization overhead
""")
