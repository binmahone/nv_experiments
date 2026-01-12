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
 * Realistic UDF Benchmark: Cross-process (Arrow+Socket) vs Embedded (JNI)
 * Tests 10 real-world UDFs with appropriate return types
 */
public class RealisticBenchmark {
    
    private static final int NUM_ROWS = 1000000;
    private static final int WARMUP_RUNS = 2;
    private static final int MEASURE_RUNS = 3;
    
    // UDF definitions for embedded Python
    private static final String[] UDF_NAMES = {
        "sentiment", "geo_parse", "ml_infer", "regex", "anomaly",
        "mask_pii", "pricing", "img_hash", "ip_lookup", "ner"
    };
    
    private static final String[] UDF_RETURN_TYPES = {
        "str", "dict", "dict", "str", "bool",
        "str", "float", "str", "dict", "list_dict"
    };
    
    // Embedded UDF code
    private static final Map<String, String> EMBEDDED_UDF_CODE = new HashMap<>();
    static {
        EMBEDDED_UDF_CODE.put("sentiment", 
            "import numpy as np\n" +
            "def udf(texts, nums):\n" +
            "    positive = {'good', 'great', 'excellent', 'happy', 'love'}\n" +
            "    negative = {'bad', 'terrible', 'hate', 'awful', 'poor'}\n" +
            "    results = []\n" +
            "    for text in texts:\n" +
            "        words = set(text.lower().split())\n" +
            "        if words & positive:\n" +
            "            results.append('positive')\n" +
            "        elif words & negative:\n" +
            "            results.append('negative')\n" +
            "        else:\n" +
            "            results.append('neutral')\n" +
            "    return results\n");
        
        EMBEDDED_UDF_CODE.put("geo_parse",
            "import numpy as np\n" +
            "def udf(texts, nums):\n" +
            "    cities = ['New York', 'Boston', 'Seattle', 'Chicago', 'Austin']\n" +
            "    states = ['NY', 'MA', 'WA', 'IL', 'TX']\n" +
            "    results = []\n" +
            "    for text in texts:\n" +
            "        idx = hash(text) % len(cities)\n" +
            "        results.append({'city': cities[idx], 'state': states[idx], 'zip': str(10000+idx)})\n" +
            "    return results\n");
        
        EMBEDDED_UDF_CODE.put("ml_infer",
            "import numpy as np\n" +
            "def udf(texts, nums):\n" +
            "    classes = ['cat', 'dog', 'bird']\n" +
            "    results = []\n" +
            "    for num in nums:\n" +
            "        probs = np.random.dirichlet(np.ones(3))\n" +
            "        results.append({'pred': classes[int(np.argmax(probs))], 'conf': float(max(probs))})\n" +
            "    return results\n");
        
        EMBEDDED_UDF_CODE.put("regex",
            "import re\n" +
            "def udf(texts, nums):\n" +
            "    pattern = re.compile(r'[\\w\\.-]+@[\\w\\.-]+\\.\\w+')\n" +
            "    results = []\n" +
            "    for text in texts:\n" +
            "        match = pattern.search(text)\n" +
            "        results.append(match.group() if match else '')\n" +
            "    return results\n");
        
        EMBEDDED_UDF_CODE.put("anomaly",
            "import numpy as np\n" +
            "def udf(texts, nums):\n" +
            "    mean, std = np.mean(nums), np.std(nums)\n" +
            "    return [bool(abs(n - mean) / std > 2) if std > 0 else False for n in nums]\n");
        
        EMBEDDED_UDF_CODE.put("mask_pii",
            "import re\n" +
            "def udf(texts, nums):\n" +
            "    email_pat = re.compile(r'[\\w\\.-]+@[\\w\\.-]+\\.\\w+')\n" +
            "    card_pat = re.compile(r'\\d{4}')\n" +
            "    results = []\n" +
            "    for text in texts:\n" +
            "        masked = email_pat.sub('[EMAIL]', text)\n" +
            "        masked = card_pat.sub('****', masked)\n" +
            "        results.append(masked)\n" +
            "    return results\n");
        
        EMBEDDED_UDF_CODE.put("pricing",
            "def udf(texts, nums):\n" +
            "    results = []\n" +
            "    for text, price in zip(texts, nums):\n" +
            "        discount = 0.1 if price > 500 else 0\n" +
            "        results.append(float(price * (1 - discount)))\n" +
            "    return results\n");
        
        EMBEDDED_UDF_CODE.put("img_hash",
            "import hashlib\n" +
            "def udf(texts, nums):\n" +
            "    results = []\n" +
            "    for text, num in zip(texts, nums):\n" +
            "        h = hashlib.md5(f'{text}{num}'.encode()).hexdigest()[:16]\n" +
            "        results.append(h)\n" +
            "    return results\n");
        
        EMBEDDED_UDF_CODE.put("ip_lookup",
            "def udf(texts, nums):\n" +
            "    countries = ['US', 'CN', 'DE', 'JP', 'GB']\n" +
            "    cities = ['Seattle', 'Beijing', 'Berlin', 'Tokyo', 'London']\n" +
            "    results = []\n" +
            "    for ip in texts:\n" +
            "        idx = hash(ip) % len(countries)\n" +
            "        results.append({'country': countries[idx], 'city': cities[idx], 'is_vpn': hash(ip) % 10 == 0})\n" +
            "    return results\n");
        
        EMBEDDED_UDF_CODE.put("ner",
            "import re\n" +
            "def udf(texts, nums):\n" +
            "    person_pat = re.compile(r'\\b[A-Z][a-z]+ [A-Z][a-z]+\\b')\n" +
            "    loc_pat = re.compile(r'\\b(?:New York|Boston|Seattle|Chicago)\\b')\n" +
            "    org_pat = re.compile(r'\\b(?:Google|Amazon|Microsoft|Apple)\\b')\n" +
            "    results = []\n" +
            "    for text in texts:\n" +
            "        entities = []\n" +
            "        for m in person_pat.finditer(text):\n" +
            "            entities.append({'text': m.group(), 'label': 'PERSON'})\n" +
            "        for m in loc_pat.finditer(text):\n" +
            "            entities.append({'text': m.group(), 'label': 'LOC'})\n" +
            "        for m in org_pat.finditer(text):\n" +
            "            entities.append({'text': m.group(), 'label': 'ORG'})\n" +
            "        results.append(entities)\n" +
            "    return results\n");
    }
    
    // Sample texts for testing
    private static final String[] SAMPLE_TEXTS = {
        "John Smith from New York called about the order yesterday.",
        "The meeting with Google executives is scheduled for Monday.",
        "Please contact support@company.com for assistance.",
        "Credit card ending in 4532 was charged $150.00.",
    };
    
    private static String[] texts;
    private static double[] nums;
    private static Random random = new Random(42);
    
    public static void main(String[] args) throws Exception {
        // Generate test data
        generateTestData();
        
        // Initialize embedded Python
        System.out.println("Initializing embedded Python...");
        EmbeddedPythonUDF.initPython();
        
        // Start Python worker
        System.out.println("Starting Python worker...");
        ProcessBuilder pb = new ProcessBuilder("python3", "python_socket_worker_v2.py", "9998");
        pb.directory(new java.io.File("."));
        pb.redirectErrorStream(true);
        Process pythonProcess = pb.start();
        
        // Wait for worker to be ready
        BufferedReader reader = new BufferedReader(new InputStreamReader(pythonProcess.getInputStream()));
        String line = reader.readLine();
        if (line == null || !line.startsWith("READY:")) {
            System.err.println("Failed to start Python worker");
            return;
        }
        int port = Integer.parseInt(line.split(":")[1]);
        System.out.println("Python worker ready on port " + port);
        
        // Connect to worker
        Socket socket = new Socket("localhost", port);
        socket.setTcpNoDelay(true);
        DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        
        BufferAllocator allocator = new RootAllocator();
        
        System.out.println("\n" + repeat("=", 110));
        System.out.println("Realistic UDF Benchmark: Cross-process (Arrow+Socket) vs Embedded (JNI)");
        System.out.println(repeat("=", 110));
        System.out.printf("Total rows: %,d | Warmup: %d | Measure: %d%n%n", NUM_ROWS, WARMUP_RUNS, MEASURE_RUNS);
        
        System.out.printf("%-12s %-10s %12s %12s %10s%n", 
            "UDF", "Return", "Cross(ms)", "Embed(ms)", "Speedup");
        System.out.println(repeat("-", 110));
        
        double totalCross = 0;
        double totalEmbed = 0;
        
        for (int i = 0; i < UDF_NAMES.length; i++) {
            String udfName = UDF_NAMES[i];
            String returnType = UDF_RETURN_TYPES[i];
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
            
            System.out.printf("%-12s %-10s %10.0fms %10.0fms %9.1fx%n",
                udfName, returnType, crossMs, embedMs, speedup);
        }
        
        System.out.println(repeat("-", 110));
        System.out.printf("%-12s %-10s %10.0fms %10.0fms %9.1fx%n",
            "TOTAL", "", totalCross, totalEmbed, totalCross / totalEmbed);
        System.out.println(repeat("=", 110));
        
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
            texts[i] = SAMPLE_TEXTS[i % SAMPLE_TEXTS.length] + " Row " + i + ".";
            nums[i] = 10 + random.nextDouble() * 990;
        }
        
        // For ip_lookup, generate IP addresses
        // We'll handle this specially in the benchmark
    }
    
    private static void runCrossProcess(String udfName, BufferAllocator allocator,
                                        DataOutputStream out, DataInputStream in) throws Exception {
        // Send EXECUTE command
        writeUTF(out, "EXECUTE");
        writeUTF(out, udfName);
        
        // Serialize input to Arrow
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
            
            // Write to bytes
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, baos)) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            byte[] arrowData = baos.toByteArray();
            
            // Send Arrow data
            out.writeInt(arrowData.length);
            out.write(arrowData);
            out.flush();
        }
        
        // Read response
        String status = readUTF(in);
        if (!"OK".equals(status)) {
            throw new RuntimeException("UDF failed: " + status);
        }
        
        int resultSize = in.readInt();
        byte[] resultData = new byte[resultSize];
        in.readFully(resultData);
        
        // Deserialize result (to complete the round trip)
        try (ArrowStreamReader reader = new ArrowStreamReader(
                new ByteArrayInputStream(resultData), allocator)) {
            reader.loadNextBatch();
            // Result is in reader.getVectorSchemaRoot()
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
