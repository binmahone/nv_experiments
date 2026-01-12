#!/usr/bin/env python3
"""
Python Socket Worker v2 - 10 Realistic UDFs
Communicates with Java via TCP Socket + Arrow IPC
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

# ============================================================================
# 10 REALISTIC UDFs with appropriate return types
# ============================================================================

def sentiment_udf(texts, nums):
    """Returns: list[str] - sentiment label"""
    positive = {'good', 'great', 'excellent', 'happy', 'love'}
    negative = {'bad', 'terrible', 'hate', 'awful', 'poor'}
    results = []
    for text in texts:
        words = set(text.lower().split())
        if words & positive:
            results.append('positive')
        elif words & negative:
            results.append('negative')
        else:
            results.append('neutral')
    return results

def geo_parse_udf(texts, nums):
    """Returns: list[dict] - structured address"""
    cities = ['New York', 'Boston', 'Seattle', 'Chicago', 'Austin']
    states = ['NY', 'MA', 'WA', 'IL', 'TX']
    results = []
    for text in texts:
        idx = hash(text) % len(cities)
        results.append({
            'city': cities[idx],
            'state': states[idx],
            'zip': str(10000 + idx)
        })
    return results

def ml_infer_udf(texts, nums):
    """Returns: list[dict] - prediction + confidence"""
    classes = ['cat', 'dog', 'bird']
    results = []
    for i, num in enumerate(nums):
        probs = np.random.dirichlet(np.ones(3))
        results.append({
            'pred': classes[int(np.argmax(probs))],
            'conf': float(max(probs))
        })
    return results

def regex_udf(texts, nums):
    """Returns: list[str] - extracted email or empty"""
    pattern = re.compile(r'[\w\.-]+@[\w\.-]+\.\w+')
    results = []
    for text in texts:
        match = pattern.search(text)
        results.append(match.group() if match else '')
    return results

def anomaly_udf(texts, nums):
    """Returns: list[bool] - is anomaly"""
    mean, std = np.mean(nums), np.std(nums)
    threshold = 2.0
    results = [bool(abs(n - mean) / std > threshold) if std > 0 else False for n in nums]
    return results

def mask_pii_udf(texts, nums):
    """Returns: list[str] - masked text"""
    email_pat = re.compile(r'[\w\.-]+@[\w\.-]+\.\w+')
    card_pat = re.compile(r'\d{4}')
    results = []
    for text in texts:
        masked = email_pat.sub('[EMAIL]', text)
        masked = card_pat.sub('****', masked)
        results.append(masked)
    return results

def pricing_udf(texts, nums):
    """Returns: list[float] - final price"""
    results = []
    for text, price in zip(texts, nums):
        discount = 0.1 if price > 500 else 0
        results.append(float(price * (1 - discount)))
    return results

def img_hash_udf(texts, nums):
    """Returns: list[str] - hash string"""
    results = []
    for text, num in zip(texts, nums):
        h = hashlib.md5(f"{text}{num}".encode()).hexdigest()[:16]
        results.append(h)
    return results

def ip_lookup_udf(texts, nums):
    """Returns: list[dict] - geo info (texts contains IPs)"""
    countries = ['US', 'CN', 'DE', 'JP', 'GB']
    cities = ['Seattle', 'Beijing', 'Berlin', 'Tokyo', 'London']
    results = []
    for ip in texts:
        idx = hash(ip) % len(countries)
        results.append({
            'country': countries[idx],
            'city': cities[idx],
            'is_vpn': hash(ip) % 10 == 0
        })
    return results

def ner_udf(texts, nums):
    """Returns: list[list[dict]] - entities per row"""
    person_pat = re.compile(r'\b[A-Z][a-z]+ [A-Z][a-z]+\b')
    loc_pat = re.compile(r'\b(?:New York|Boston|Seattle|Chicago)\b')
    org_pat = re.compile(r'\b(?:Google|Amazon|Microsoft|Apple)\b')
    results = []
    for text in texts:
        entities = []
        for m in person_pat.finditer(text):
            entities.append({'text': m.group(), 'label': 'PERSON', 'start': m.start(), 'end': m.end()})
        for m in loc_pat.finditer(text):
            entities.append({'text': m.group(), 'label': 'LOC', 'start': m.start(), 'end': m.end()})
        for m in org_pat.finditer(text):
            entities.append({'text': m.group(), 'label': 'ORG', 'start': m.start(), 'end': m.end()})
        results.append(entities)
    return results

# UDF registry
UDFS = {
    'sentiment': sentiment_udf,
    'geo_parse': geo_parse_udf,
    'ml_infer': ml_infer_udf,
    'regex': regex_udf,
    'anomaly': anomaly_udf,
    'mask_pii': mask_pii_udf,
    'pricing': pricing_udf,
    'img_hash': img_hash_udf,
    'ip_lookup': ip_lookup_udf,
    'ner': ner_udf,
}

# Return type registry
RETURN_TYPES = {
    'sentiment': 'str',
    'geo_parse': 'dict',
    'ml_infer': 'dict',
    'regex': 'str',
    'anomaly': 'bool',
    'mask_pii': 'str',
    'pricing': 'float',
    'img_hash': 'str',
    'ip_lookup': 'dict',
    'ner': 'list_dict',
}

# ============================================================================
# Socket communication helpers
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

# ============================================================================
# Main worker loop
# ============================================================================

def convert_result_to_arrow(result, rtype):
    """Convert UDF result to Arrow array based on return type"""
    if rtype == 'str':
        return pa.array(result, type=pa.utf8())
    elif rtype == 'bool':
        return pa.array(result, type=pa.bool_())
    elif rtype == 'float':
        return pa.array(result, type=pa.float64())
    elif rtype == 'dict':
        # dict -> JSON string
        json_strs = [json.dumps(r) for r in result]
        return pa.array(json_strs, type=pa.utf8())
    elif rtype == 'list_dict':
        # list[dict] -> JSON string
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
    
    # Signal ready
    print(f"READY:{port}", flush=True)
    
    conn, addr = server.accept()
    
    # Set socket options for better performance
    conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    
    try:
        while True:
            # Read command
            cmd = read_utf(conn)
            if cmd is None or cmd == 'SHUTDOWN':
                break
            
            if cmd == 'EXECUTE':
                # Read UDF name
                udf_name = read_utf(conn)
                
                # Read Arrow data size
                arrow_size = read_int(conn)
                
                # Read Arrow data
                arrow_data = read_bytes(conn, arrow_size)
                
                # Deserialize Arrow
                reader = ipc.open_stream(io.BytesIO(arrow_data))
                table = reader.read_all()
                
                # Extract columns
                texts = table.column('texts').to_pylist()
                nums = table.column('nums').to_numpy()
                
                # Execute UDF
                udf_func = UDFS.get(udf_name)
                if udf_func is None:
                    write_utf(conn, f"ERROR: Unknown UDF {udf_name}")
                    continue
                
                result = udf_func(texts, nums)
                
                # Convert result to Arrow
                rtype = RETURN_TYPES.get(udf_name, 'str')
                result_array = convert_result_to_arrow(result, rtype)
                
                result_table = pa.table({'result': result_array})
                
                # Serialize to Arrow IPC
                sink = io.BytesIO()
                with ipc.new_stream(sink, result_table.schema) as writer:
                    writer.write_table(result_table)
                result_bytes = sink.getvalue()
                
                # Send result
                write_utf(conn, 'OK')
                write_int(conn, len(result_bytes))
                conn.sendall(result_bytes)
    
    finally:
        conn.close()
        server.close()

if __name__ == '__main__':
    main()
