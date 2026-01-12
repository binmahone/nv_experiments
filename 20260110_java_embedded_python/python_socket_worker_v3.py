#!/usr/bin/env python3
"""
Python Socket Worker v3 - Realistic UDFs using Python stdlib
These UDFs represent real business logic that users write in Python
because they need Python's specific behaviors (regex, string ops, dict ordering, etc.)
"""

import sys
import socket
import struct
import io
import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc
import re
import json
import hashlib
import base64
import urllib.parse
from datetime import datetime, timedelta
from collections import OrderedDict

# ============================================================================
# 10 REALISTIC UDFs using Python stdlib
# ============================================================================

def extract_fields_udf(texts, nums):
    """
    Extract multiple fields from semi-structured text using regex.
    Real scenario: Parse log lines, extract order info from emails, etc.
    Returns: dict with extracted fields
    """
    email_pat = re.compile(r'[\w\.-]+@[\w\.-]+\.\w+')
    phone_pat = re.compile(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b')
    amount_pat = re.compile(r'\$[\d,]+(?:\.\d{2})?')
    date_pat = re.compile(r'\b\d{1,2}/\d{1,2}/\d{2,4}\b')
    order_pat = re.compile(r'#(\d{5,})')
    
    results = []
    for text in texts:
        emails = email_pat.findall(text)
        phones = phone_pat.findall(text)
        amounts = amount_pat.findall(text)
        dates = date_pat.findall(text)
        orders = order_pat.findall(text)
        
        results.append({
            'emails': emails,
            'phones': phones,
            'amounts': amounts,
            'dates': dates,
            'order_ids': orders,
            'has_contact': len(emails) > 0 or len(phones) > 0
        })
    return results


def parse_json_udf(texts, nums):
    """
    Parse JSON strings and extract/transform nested fields.
    Real scenario: Parse API responses, config strings, event payloads.
    Returns: dict with flattened fields
    """
    results = []
    for text, num in zip(texts, nums):
        # Simulate JSON parsing (text is not real JSON, so we construct one)
        # In real scenario, text would be actual JSON string
        fake_json = {
            'user': {'name': text[:20], 'id': int(num)},
            'event': 'click' if num > 500 else 'view',
            'timestamp': '2024-01-15T10:30:00Z',
            'metadata': {'source': 'web', 'version': '2.0'}
        }
        
        # Flatten and transform
        results.append({
            'user_name': fake_json['user']['name'],
            'user_id': fake_json['user']['id'],
            'event_type': fake_json['event'],
            'source': fake_json['metadata']['source'],
            'is_click': fake_json['event'] == 'click'
        })
    return results


def normalize_text_udf(texts, nums):
    """
    Clean and normalize text: remove extra spaces, lowercase, strip punctuation.
    Real scenario: Prepare text for search indexing, deduplication, comparison.
    Returns: str (cleaned text)
    """
    punct_pat = re.compile(r'[^\w\s]')
    space_pat = re.compile(r'\s+')
    
    results = []
    for text in texts:
        # Lowercase
        cleaned = text.lower()
        # Remove punctuation
        cleaned = punct_pat.sub('', cleaned)
        # Collapse whitespace
        cleaned = space_pat.sub(' ', cleaned).strip()
        results.append(cleaned)
    return results


def parse_date_udf(texts, nums):
    """
    Parse dates from various formats and compute derived fields.
    Real scenario: ETL pipelines normalizing date formats from different sources.
    Returns: dict with parsed date components
    """
    formats = ['%m/%d/%Y', '%Y-%m-%d', '%d-%m-%Y', '%B %d, %Y']
    
    results = []
    for i, text in enumerate(texts):
        # Extract date-like string or use a default
        date_match = re.search(r'\d{1,2}/\d{1,2}/\d{2,4}', text)
        if date_match:
            date_str = date_match.group()
            try:
                dt = datetime.strptime(date_str, '%m/%d/%Y')
            except:
                dt = datetime(2024, 1, 15)
        else:
            # Generate a date based on row number
            dt = datetime(2024, 1, 1) + timedelta(days=i % 365)
        
        results.append({
            'year': dt.year,
            'month': dt.month,
            'day': dt.day,
            'weekday': dt.strftime('%A'),
            'quarter': (dt.month - 1) // 3 + 1,
            'is_weekend': dt.weekday() >= 5
        })
    return results


def url_parse_udf(texts, nums):
    """
    Parse URLs and extract components.
    Real scenario: Analyze web traffic logs, extract domains for filtering.
    Returns: dict with URL components
    """
    results = []
    for i, text in enumerate(texts):
        # Construct a URL-like string
        domains = ['example.com', 'test.org', 'shop.io', 'api.dev', 'data.net']
        paths = ['/home', '/products', '/api/v1/users', '/checkout', '/search']
        
        url = f"https://{domains[i % len(domains)]}{paths[i % len(paths)]}?id={int(nums[i])}&ref=email"
        
        parsed = urllib.parse.urlparse(url)
        query_params = urllib.parse.parse_qs(parsed.query)
        
        results.append({
            'scheme': parsed.scheme,
            'domain': parsed.netloc,
            'path': parsed.path,
            'params': {k: v[0] for k, v in query_params.items()},
            'is_api': '/api/' in parsed.path
        })
    return results


def validate_format_udf(texts, nums):
    """
    Validate data formats: email, phone, credit card (Luhn), etc.
    Real scenario: Data quality checks, input validation before insert.
    Returns: dict with validation results
    """
    email_pat = re.compile(r'^[\w\.-]+@[\w\.-]+\.\w{2,}$')
    phone_pat = re.compile(r'^\d{10}$|^\d{3}-\d{3}-\d{4}$')
    
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
    
    results = []
    for text, num in zip(texts, nums):
        # Check various formats in text
        has_valid_email = bool(email_pat.search(text))
        
        # Generate a test credit card number
        cc_num = f"{int(num):016d}"[:16]
        
        results.append({
            'has_email': 'email' in text.lower() or '@' in text,
            'valid_email_format': has_valid_email,
            'cc_luhn_valid': luhn_check(cc_num),
            'text_length_ok': 10 <= len(text) <= 500
        })
    return results


def encode_decode_udf(texts, nums):
    """
    Base64 encode/decode, URL encode, hash generation.
    Real scenario: Generate tokens, encode data for APIs, create checksums.
    Returns: dict with encoded values
    """
    results = []
    for text, num in zip(texts, nums):
        text_bytes = text.encode('utf-8')
        
        # Base64 encode
        b64 = base64.b64encode(text_bytes).decode('ascii')
        
        # URL encode
        url_encoded = urllib.parse.quote(text)
        
        # Multiple hashes
        md5 = hashlib.md5(text_bytes).hexdigest()
        sha1 = hashlib.sha1(text_bytes).hexdigest()[:16]
        
        # HMAC-like signature
        key = str(int(num)).encode()
        signature = hashlib.sha256(key + text_bytes).hexdigest()[:32]
        
        results.append({
            'b64': b64[:50],  # Truncate for readability
            'url_encoded': url_encoded[:50],
            'md5': md5,
            'sha1': sha1,
            'signature': signature
        })
    return results


def business_rules_udf(texts, nums):
    """
    Complex business rule evaluation with multiple conditions.
    Real scenario: Pricing rules, eligibility checks, fraud scoring.
    Returns: dict with rule evaluation results
    """
    results = []
    for text, num in zip(texts, nums):
        # Multiple business rules
        is_premium = num > 500
        is_new_customer = 'new' in text.lower() or 'first' in text.lower()
        is_bulk_order = num > 800
        has_promo = 'promo' in text.lower() or 'discount' in text.lower()
        
        # Calculate discount tiers
        discount = 0
        if is_new_customer:
            discount += 10
        if is_bulk_order:
            discount += 15
        if has_promo:
            discount += 5
        discount = min(discount, 25)  # Cap at 25%
        
        # Risk score
        risk_factors = 0
        if num > 900:
            risk_factors += 2
        if len(text) < 20:
            risk_factors += 1
        if '@' not in text:
            risk_factors += 1
        
        final_price = num * (1 - discount / 100)
        
        results.append({
            'tier': 'gold' if is_premium else 'silver',
            'discount_pct': discount,
            'final_price': round(final_price, 2),
            'risk_score': risk_factors,
            'approved': risk_factors < 3
        })
    return results


def text_transform_udf(texts, nums):
    """
    Various string transformations: title case, slug generation, truncation.
    Real scenario: Generate display names, URL slugs, preview text.
    Returns: dict with transformed strings
    """
    results = []
    for text, num in zip(texts, nums):
        # Title case
        title = text.title()
        
        # Generate URL slug
        slug = re.sub(r'[^\w\s-]', '', text.lower())
        slug = re.sub(r'[\s_]+', '-', slug).strip('-')[:50]
        
        # Truncate with ellipsis
        max_len = 30
        preview = text[:max_len] + '...' if len(text) > max_len else text
        
        # Word count and char count
        words = text.split()
        
        # Initials
        initials = ''.join(w[0].upper() for w in words[:3] if w)
        
        results.append({
            'title_case': title[:50],
            'slug': slug,
            'preview': preview,
            'word_count': len(words),
            'char_count': len(text),
            'initials': initials
        })
    return results


def data_mask_udf(texts, nums):
    """
    Mask sensitive data with various strategies.
    Real scenario: GDPR compliance, log sanitization, data anonymization.
    Returns: str (masked text)
    """
    # Patterns for sensitive data
    email_pat = re.compile(r'([\w\.-]+)@([\w\.-]+\.\w+)')
    phone_pat = re.compile(r'\b(\d{3})[-.]?(\d{3})[-.]?(\d{4})\b')
    cc_pat = re.compile(r'\b(\d{4})[-\s]?(\d{4})[-\s]?(\d{4})[-\s]?(\d{4})\b')
    ssn_pat = re.compile(r'\b(\d{3})[-]?(\d{2})[-]?(\d{4})\b')
    
    results = []
    for text in texts:
        masked = text
        
        # Mask email: keep first char and domain
        masked = email_pat.sub(r'\1[...]@\2', masked)
        
        # Mask phone: keep last 4
        masked = phone_pat.sub(r'***-***-\3', masked)
        
        # Mask credit card: keep last 4
        masked = cc_pat.sub(r'****-****-****-\4', masked)
        
        # Mask SSN: keep last 4
        masked = ssn_pat.sub(r'***-**-\3', masked)
        
        results.append(masked)
    return results


# UDF registry
UDFS = {
    'extract_fields': extract_fields_udf,
    'parse_json': parse_json_udf,
    'normalize_text': normalize_text_udf,
    'parse_date': parse_date_udf,
    'url_parse': url_parse_udf,
    'validate_format': validate_format_udf,
    'encode_decode': encode_decode_udf,
    'business_rules': business_rules_udf,
    'text_transform': text_transform_udf,
    'data_mask': data_mask_udf,
}

# Return type registry
RETURN_TYPES = {
    'extract_fields': 'dict',
    'parse_json': 'dict',
    'normalize_text': 'str',
    'parse_date': 'dict',
    'url_parse': 'dict',
    'validate_format': 'dict',
    'encode_decode': 'dict',
    'business_rules': 'dict',
    'text_transform': 'dict',
    'data_mask': 'str',
}

# ============================================================================
# Socket communication (same as before)
# ============================================================================

def read_int(sock):
    data = sock.recv(4)
    if len(data) < 4:
        return None
    return struct.unpack('>i', data)[0]

def write_int(sock, val):
    sock.sendall(struct.pack('>i', val))

def read_utf(sock):
    length = read_int(sock)
    if length is None or length <= 0:
        return None
    data = b''
    while len(data) < length:
        chunk = sock.recv(length - len(data))
        if not chunk:
            return None
        data += chunk
    return data.decode('utf-8')

def write_utf(sock, s):
    data = s.encode('utf-8')
    write_int(sock, len(data))
    sock.sendall(data)

def read_bytes(sock, length):
    data = b''
    while len(data) < length:
        chunk = sock.recv(min(65536, length - len(data)))
        if not chunk:
            return None
        data += chunk
    return data

def convert_result_to_arrow(result, rtype):
    if rtype == 'str':
        return pa.array(result, type=pa.utf8())
    elif rtype == 'bool':
        return pa.array(result, type=pa.bool_())
    elif rtype == 'float':
        return pa.array(result, type=pa.float64())
    elif rtype in ('dict', 'list_dict'):
        json_strs = [json.dumps(r) for r in result]
        return pa.array(json_strs, type=pa.utf8())
    else:
        return pa.array([str(r) for r in result], type=pa.utf8())

def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9999
    
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('localhost', port))
    server.listen(1)
    
    print(f"READY:{port}", flush=True)
    
    conn, addr = server.accept()
    conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    
    try:
        while True:
            cmd = read_utf(conn)
            if cmd is None or cmd == 'SHUTDOWN':
                break
            
            if cmd == 'EXECUTE':
                udf_name = read_utf(conn)
                arrow_size = read_int(conn)
                arrow_data = read_bytes(conn, arrow_size)
                
                reader = ipc.open_stream(io.BytesIO(arrow_data))
                table = reader.read_all()
                
                texts = table.column('texts').to_pylist()
                nums = table.column('nums').to_numpy()
                
                udf_func = UDFS.get(udf_name)
                if udf_func is None:
                    write_utf(conn, f"ERROR: Unknown UDF {udf_name}")
                    continue
                
                result = udf_func(texts, nums)
                
                rtype = RETURN_TYPES.get(udf_name, 'str')
                result_array = convert_result_to_arrow(result, rtype)
                
                result_table = pa.table({'result': result_array})
                
                sink = io.BytesIO()
                with ipc.new_stream(sink, result_table.schema) as writer:
                    writer.write_table(result_table)
                result_bytes = sink.getvalue()
                
                write_utf(conn, 'OK')
                write_int(conn, len(result_bytes))
                conn.sendall(result_bytes)
    
    finally:
        conn.close()
        server.close()

if __name__ == '__main__':
    main()
