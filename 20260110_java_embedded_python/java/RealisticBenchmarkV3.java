import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.*;
import org.apache.arrow.vector.types.pojo.*;
import org.apache.arrow.vector.types.FloatingPointPrecision;

/**
 * Realistic UDF Benchmark V3: Python stdlib-based UDFs
 * These represent real business logic using regex, string ops, datetime, etc.
 */
public class RealisticBenchmarkV3 {
    
    private static final int NUM_ROWS = 100000;
    private static final int WARMUP_RUNS = 2;
    private static final int MEASURE_RUNS = 3;
    
    // UDF names and their return types
    private static final String[][] UDFS = {
        {"extract_fields", "dict", "Multi-regex extraction (email/phone/amount/date)"},
        {"parse_json", "dict", "JSON parse and field transformation"},
        {"normalize_text", "str", "Text cleaning (lowercase/punct/whitespace)"},
        {"parse_date", "dict", "Date parsing and derived fields"},
        {"url_parse", "dict", "URL parsing (domain/path/params)"},
        {"validate_format", "dict", "Format validation (email/CC Luhn check)"},
        {"encode_decode", "dict", "Base64/URL encode + hash generation"},
        {"business_rules", "dict", "Complex rule evaluation (pricing/risk)"},
        {"text_transform", "dict", "String transforms (slug/title/truncate)"},
        {"data_mask", "str", "PII masking (email/phone/SSN/CC)"},
    };
    
    // Embedded UDF code
    private static final Map<String, String> EMBEDDED_UDF_CODE = new HashMap<>();
    static {
        EMBEDDED_UDF_CODE.put("extract_fields",
            "import re\n" +
            "def udf(texts, nums):\n" +
            "    email_pat = re.compile(r'[\\w\\.-]+@[\\w\\.-]+\\.\\w+')\n" +
            "    phone_pat = re.compile(r'\\b\\d{3}[-.]?\\d{3}[-.]?\\d{4}\\b')\n" +
            "    amount_pat = re.compile(r'\\$[\\d,]+(?:\\.\\d{2})?')\n" +
            "    date_pat = re.compile(r'\\b\\d{1,2}/\\d{1,2}/\\d{2,4}\\b')\n" +
            "    order_pat = re.compile(r'#(\\d{5,})')\n" +
            "    results = []\n" +
            "    for text in texts:\n" +
            "        results.append({\n" +
            "            'emails': email_pat.findall(text),\n" +
            "            'phones': phone_pat.findall(text),\n" +
            "            'amounts': amount_pat.findall(text),\n" +
            "            'dates': date_pat.findall(text),\n" +
            "            'order_ids': order_pat.findall(text),\n" +
            "            'has_contact': bool(email_pat.search(text) or phone_pat.search(text))\n" +
            "        })\n" +
            "    return results\n");
        
        EMBEDDED_UDF_CODE.put("parse_json",
            "def udf(texts, nums):\n" +
            "    results = []\n" +
            "    for text, num in zip(texts, nums):\n" +
            "        fake_json = {\n" +
            "            'user': {'name': text[:20], 'id': int(num)},\n" +
            "            'event': 'click' if num > 500 else 'view',\n" +
            "            'metadata': {'source': 'web'}\n" +
            "        }\n" +
            "        results.append({\n" +
            "            'user_name': fake_json['user']['name'],\n" +
            "            'user_id': fake_json['user']['id'],\n" +
            "            'event_type': fake_json['event'],\n" +
            "            'source': fake_json['metadata']['source'],\n" +
            "            'is_click': fake_json['event'] == 'click'\n" +
            "        })\n" +
            "    return results\n");
        
        EMBEDDED_UDF_CODE.put("normalize_text",
            "import re\n" +
            "def udf(texts, nums):\n" +
            "    punct_pat = re.compile(r'[^\\w\\s]')\n" +
            "    space_pat = re.compile(r'\\s+')\n" +
            "    results = []\n" +
            "    for text in texts:\n" +
            "        cleaned = text.lower()\n" +
            "        cleaned = punct_pat.sub('', cleaned)\n" +
            "        cleaned = space_pat.sub(' ', cleaned).strip()\n" +
            "        results.append(cleaned)\n" +
            "    return results\n");
        
        EMBEDDED_UDF_CODE.put("parse_date",
            "import re\n" +
            "from datetime import datetime, timedelta\n" +
            "def udf(texts, nums):\n" +
            "    results = []\n" +
            "    for i, text in enumerate(texts):\n" +
            "        date_match = re.search(r'\\d{1,2}/\\d{1,2}/\\d{2,4}', text)\n" +
            "        if date_match:\n" +
            "            try:\n" +
            "                dt = datetime.strptime(date_match.group(), '%m/%d/%Y')\n" +
            "            except:\n" +
            "                dt = datetime(2024, 1, 15)\n" +
            "        else:\n" +
            "            dt = datetime(2024, 1, 1) + timedelta(days=i % 365)\n" +
            "        results.append({\n" +
            "            'year': dt.year, 'month': dt.month, 'day': dt.day,\n" +
            "            'weekday': dt.strftime('%A'),\n" +
            "            'quarter': (dt.month - 1) // 3 + 1,\n" +
            "            'is_weekend': dt.weekday() >= 5\n" +
            "        })\n" +
            "    return results\n");
        
        EMBEDDED_UDF_CODE.put("url_parse",
            "import urllib.parse\n" +
            "def udf(texts, nums):\n" +
            "    domains = ['example.com', 'test.org', 'shop.io', 'api.dev', 'data.net']\n" +
            "    paths = ['/home', '/products', '/api/v1/users', '/checkout', '/search']\n" +
            "    results = []\n" +
            "    for i, (text, num) in enumerate(zip(texts, nums)):\n" +
            "        url = f'https://{domains[i % len(domains)]}{paths[i % len(paths)]}?id={int(num)}&ref=email'\n" +
            "        parsed = urllib.parse.urlparse(url)\n" +
            "        query_params = urllib.parse.parse_qs(parsed.query)\n" +
            "        results.append({\n" +
            "            'scheme': parsed.scheme, 'domain': parsed.netloc, 'path': parsed.path,\n" +
            "            'params': {k: v[0] for k, v in query_params.items()},\n" +
            "            'is_api': '/api/' in parsed.path\n" +
            "        })\n" +
            "    return results\n");
        
        EMBEDDED_UDF_CODE.put("validate_format",
            "import re\n" +
            "def udf(texts, nums):\n" +
            "    email_pat = re.compile(r'^[\\w\\.-]+@[\\w\\.-]+\\.\\w{2,}$')\n" +
            "    def luhn_check(num_str):\n" +
            "        digits = [int(d) for d in num_str if d.isdigit()]\n" +
            "        if len(digits) < 13: return False\n" +
            "        checksum = 0\n" +
            "        for i, d in enumerate(reversed(digits)):\n" +
            "            if i % 2 == 1:\n" +
            "                d *= 2\n" +
            "                if d > 9: d -= 9\n" +
            "            checksum += d\n" +
            "        return checksum % 10 == 0\n" +
            "    results = []\n" +
            "    for text, num in zip(texts, nums):\n" +
            "        cc_num = f'{int(num):016d}'[:16]\n" +
            "        results.append({\n" +
            "            'has_email': '@' in text,\n" +
            "            'valid_email_format': bool(email_pat.search(text)),\n" +
            "            'cc_luhn_valid': luhn_check(cc_num),\n" +
            "            'text_length_ok': 10 <= len(text) <= 500\n" +
            "        })\n" +
            "    return results\n");
        
        EMBEDDED_UDF_CODE.put("encode_decode",
            "import base64\nimport urllib.parse\nimport hashlib\n" +
            "def udf(texts, nums):\n" +
            "    results = []\n" +
            "    for text, num in zip(texts, nums):\n" +
            "        text_bytes = text.encode('utf-8')\n" +
            "        b64 = base64.b64encode(text_bytes).decode('ascii')\n" +
            "        url_encoded = urllib.parse.quote(text)\n" +
            "        md5 = hashlib.md5(text_bytes).hexdigest()\n" +
            "        sha1 = hashlib.sha1(text_bytes).hexdigest()[:16]\n" +
            "        key = str(int(num)).encode()\n" +
            "        signature = hashlib.sha256(key + text_bytes).hexdigest()[:32]\n" +
            "        results.append({\n" +
            "            'b64': b64[:50], 'url_encoded': url_encoded[:50],\n" +
            "            'md5': md5, 'sha1': sha1, 'signature': signature\n" +
            "        })\n" +
            "    return results\n");
        
        EMBEDDED_UDF_CODE.put("business_rules",
            "def udf(texts, nums):\n" +
            "    results = []\n" +
            "    for text, num in zip(texts, nums):\n" +
            "        is_premium = num > 500\n" +
            "        is_new = 'new' in text.lower() or 'first' in text.lower()\n" +
            "        is_bulk = num > 800\n" +
            "        has_promo = 'promo' in text.lower() or 'discount' in text.lower()\n" +
            "        discount = 0\n" +
            "        if is_new: discount += 10\n" +
            "        if is_bulk: discount += 15\n" +
            "        if has_promo: discount += 5\n" +
            "        discount = min(discount, 25)\n" +
            "        risk = 0\n" +
            "        if num > 900: risk += 2\n" +
            "        if len(text) < 20: risk += 1\n" +
            "        if '@' not in text: risk += 1\n" +
            "        results.append({\n" +
            "            'tier': 'gold' if is_premium else 'silver',\n" +
            "            'discount_pct': discount,\n" +
            "            'final_price': round(num * (1 - discount/100), 2),\n" +
            "            'risk_score': risk, 'approved': risk < 3\n" +
            "        })\n" +
            "    return results\n");
        
        EMBEDDED_UDF_CODE.put("text_transform",
            "import re\n" +
            "def udf(texts, nums):\n" +
            "    results = []\n" +
            "    for text, num in zip(texts, nums):\n" +
            "        title = text.title()\n" +
            "        slug = re.sub(r'[^\\w\\s-]', '', text.lower())\n" +
            "        slug = re.sub(r'[\\s_]+', '-', slug).strip('-')[:50]\n" +
            "        preview = text[:30] + '...' if len(text) > 30 else text\n" +
            "        words = text.split()\n" +
            "        initials = ''.join(w[0].upper() for w in words[:3] if w)\n" +
            "        results.append({\n" +
            "            'title_case': title[:50], 'slug': slug, 'preview': preview,\n" +
            "            'word_count': len(words), 'char_count': len(text), 'initials': initials\n" +
            "        })\n" +
            "    return results\n");
        
        EMBEDDED_UDF_CODE.put("data_mask",
            "import re\n" +
            "def udf(texts, nums):\n" +
            "    email_pat = re.compile(r'([\\w\\.-]+)@([\\w\\.-]+\\.\\w+)')\n" +
            "    phone_pat = re.compile(r'\\b(\\d{3})[-.]?(\\d{3})[-.]?(\\d{4})\\b')\n" +
            "    cc_pat = re.compile(r'\\b(\\d{4})[-\\s]?(\\d{4})[-\\s]?(\\d{4})[-\\s]?(\\d{4})\\b')\n" +
            "    ssn_pat = re.compile(r'\\b(\\d{3})[-]?(\\d{2})[-]?(\\d{4})\\b')\n" +
            "    results = []\n" +
            "    for text in texts:\n" +
            "        masked = text\n" +
            "        masked = email_pat.sub(r'\\1[...]@\\2', masked)\n" +
            "        masked = phone_pat.sub(r'***-***-\\3', masked)\n" +
            "        masked = cc_pat.sub(r'****-****-****-\\4', masked)\n" +
            "        masked = ssn_pat.sub(r'***-**-\\3', masked)\n" +
            "        results.append(masked)\n" +
            "    return results\n");
    }
    
    // Sample texts with realistic content
    private static final String[] SAMPLE_TEXTS = {
        "Contact John Smith at john.smith@example.com or call 555-123-4567 for order #12345",
        "Payment of $1,250.00 received on 01/15/2024 from new customer account",
        "Shipping to 123 Main St, New York, NY 10001. Promo code SAVE20 applied.",
        "Credit card ending 4532 charged $99.99. Reference: TXN-2024-001",
        "Meeting scheduled with Google team on 02/20/2024 at headquarters",
        "Support ticket from premium user: discount request for bulk order of 50 units",
        "API call to https://api.example.com/v1/users?id=12345&token=abc",
        "SSN: 123-45-6789, DOB: 03/15/1990, Email: user@test.com",
    };
    
    private static String[] texts;
    private static double[] nums;
    private static Random random = new Random(42);
    
    public static void main(String[] args) throws Exception {
        generateTestData();
        
        System.out.println("Initializing embedded Python...");
        EmbeddedPythonUDF.initPython();
        
        System.out.println("Starting Python worker...");
        ProcessBuilder pb = new ProcessBuilder("python3", "python_socket_worker_v3.py", "9997");
        pb.directory(new java.io.File("."));
        pb.redirectErrorStream(true);
        Process pythonProcess = pb.start();
        
        BufferedReader reader = new BufferedReader(new InputStreamReader(pythonProcess.getInputStream()));
        String line = reader.readLine();
        if (line == null || !line.startsWith("READY:")) {
            System.err.println("Failed to start Python worker: " + line);
            return;
        }
        int port = Integer.parseInt(line.split(":")[1]);
        System.out.println("Python worker ready on port " + port);
        
        Socket socket = new Socket("localhost", port);
        socket.setTcpNoDelay(true);
        DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        
        BufferAllocator allocator = new RootAllocator();
        
        System.out.println("\n" + repeat("=", 120));
        System.out.println("Realistic UDF Benchmark V3: Python stdlib-based UDFs");
        System.out.println(repeat("=", 120));
        System.out.printf("Rows: %,d | Warmup: %d | Measure: %d%n%n", NUM_ROWS, WARMUP_RUNS, MEASURE_RUNS);
        
        System.out.printf("%-16s %-6s %-42s %10s %10s %8s%n", 
            "UDF", "Type", "Description", "Cross(ms)", "Embed(ms)", "Speedup");
        System.out.println(repeat("-", 120));
        
        double totalCross = 0;
        double totalEmbed = 0;
        
        for (String[] udfInfo : UDFS) {
            String udfName = udfInfo[0];
            String returnType = udfInfo[1];
            String description = udfInfo[2];
            String udfCode = EMBEDDED_UDF_CODE.get(udfName);
            
            // Warmup
            for (int w = 0; w < WARMUP_RUNS; w++) {
                runCrossProcess(udfName, allocator, out, in);
                EmbeddedPythonUDF.benchmarkEmbeddedGenericUDF(texts, nums, udfCode, 1);
            }
            
            // Measure Cross-process
            long crossTotal = 0;
            for (int r = 0; r < MEASURE_RUNS; r++) {
                long start = System.nanoTime();
                runCrossProcess(udfName, allocator, out, in);
                crossTotal += System.nanoTime() - start;
            }
            double crossMs = crossTotal / MEASURE_RUNS / 1_000_000.0;
            
            // Measure Embedded
            long embedTotal = 0;
            for (int r = 0; r < MEASURE_RUNS; r++) {
                long start = System.nanoTime();
                EmbeddedPythonUDF.benchmarkEmbeddedGenericUDF(texts, nums, udfCode, 1);
                embedTotal += System.nanoTime() - start;
            }
            double embedMs = embedTotal / MEASURE_RUNS / 1_000_000.0;
            
            double speedup = crossMs / embedMs;
            totalCross += crossMs;
            totalEmbed += embedMs;
            
            System.out.printf("%-16s %-6s %-42s %8.0fms %8.0fms %7.2fx%n",
                udfName, returnType, description, crossMs, embedMs, speedup);
        }
        
        System.out.println(repeat("-", 120));
        System.out.printf("%-16s %-6s %-42s %8.0fms %8.0fms %7.2fx%n",
            "TOTAL", "", "", totalCross, totalEmbed, totalCross / totalEmbed);
        System.out.println(repeat("=", 120));
        
        // Shutdown
        writeUTF(out, "SHUTDOWN");
        out.flush();
        socket.close();
        pythonProcess.waitFor();
        allocator.close();
        
        System.out.println("\nDone.");
    }
    
    private static void generateTestData() {
        texts = new String[NUM_ROWS];
        nums = new double[NUM_ROWS];
        
        for (int i = 0; i < NUM_ROWS; i++) {
            texts[i] = SAMPLE_TEXTS[i % SAMPLE_TEXTS.length] + " Row " + i;
            nums[i] = 10 + random.nextDouble() * 990;
        }
    }
    
    private static void runCrossProcess(String udfName, BufferAllocator allocator,
                                        DataOutputStream out, DataInputStream in) throws Exception {
        writeUTF(out, "EXECUTE");
        writeUTF(out, udfName);
        
        Schema schema = new Schema(Arrays.asList(
            Field.nullable("texts", new ArrowType.Utf8()),
            Field.nullable("nums", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))
        ));
        
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            VarCharVector textVector = (VarCharVector) root.getVector("texts");
            Float8Vector numVector = (Float8Vector) root.getVector("nums");
            
            textVector.allocateNew(NUM_ROWS * 100, NUM_ROWS);
            numVector.allocateNew(NUM_ROWS);
            
            for (int i = 0; i < NUM_ROWS; i++) {
                byte[] bytes = texts[i].getBytes(StandardCharsets.UTF_8);
                textVector.setSafe(i, bytes);
                numVector.setSafe(i, nums[i]);
            }
            
            root.setRowCount(NUM_ROWS);
            
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, baos)) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            byte[] arrowData = baos.toByteArray();
            
            out.writeInt(arrowData.length);
            out.write(arrowData);
            out.flush();
        }
        
        String status = readUTF(in);
        if (!"OK".equals(status)) {
            throw new RuntimeException("UDF failed: " + status);
        }
        
        int resultSize = in.readInt();
        byte[] resultData = new byte[resultSize];
        in.readFully(resultData);
        
        try (ArrowStreamReader reader = new ArrowStreamReader(
                new ByteArrayInputStream(resultData), allocator)) {
            reader.loadNextBatch();
        }
    }
    
    private static void writeUTF(DataOutputStream out, String s) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }
    
    private static String readUTF(DataInputStream in) throws IOException {
        int length = in.readInt();
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
    
    private static String repeat(String s, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(s);
        }
        return sb.toString();
    }
}
