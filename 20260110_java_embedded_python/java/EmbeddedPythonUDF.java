import java.io.*;
import java.net.*;
import java.util.Random;

/**
 * 10 Real-World UDF Benchmark: Embedded vs Cross-Process (with Arrow)
 */
public class EmbeddedPythonUDF {
    
    static { System.loadLibrary("embedded_python_udf"); }
    
    public static native void initPython();
    public static native void finalizePython();
    public static native long benchmarkEmbeddedGenericUDF(
        String[] col1, double[] col2, String udfCode, int iterations);
    
    static class PythonWorkerConnection implements AutoCloseable {
        private Process process;
        private ServerSocket serverSocket;
        private Socket socket;
        private DataInputStream in;
        private DataOutputStream out;
        
        public PythonWorkerConnection(boolean useArrow) throws Exception {
            serverSocket = new ServerSocket(0);
            ProcessBuilder pb = new ProcessBuilder("python3",
                System.getProperty("user.dir") + "/python_worker_arrow.py",
                String.valueOf(serverSocket.getLocalPort()),
                useArrow ? "arrow" : "binary");
            pb.redirectErrorStream(true);
            process = pb.start();
            serverSocket.setSoTimeout(15000);
            socket = serverSocket.accept();
            in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        }
        
        public void setUDF(String udfCode) throws IOException {
            out.writeByte(0);
            byte[] bytes = udfCode.getBytes("UTF-8");
            out.writeInt(bytes.length);
            out.write(bytes);
            out.flush();
            in.readByte();
        }
        
        public void executeUDF(String[] strings, double[] numbers) throws IOException {
            out.writeByte(1);
            // Send strings
            out.writeInt(strings.length);
            for (String s : strings) {
                byte[] bytes = s.getBytes("UTF-8");
                out.writeInt(bytes.length);
                out.write(bytes);
            }
            // Send numbers
            out.writeInt(numbers.length);
            for (double d : numbers) out.writeDouble(d);
            out.flush();
            // Read result count and discard
            int len = in.readInt();
            for (int i = 0; i < len; i++) {
                int strLen = in.readInt();
                in.readFully(new byte[strLen]);
            }
        }
        
        @Override
        public void close() throws Exception {
            try { out.writeByte(2); out.flush(); } catch (Exception e) {}
            if (socket != null) socket.close();
            if (serverSocket != null) serverSocket.close();
            if (process != null) { process.waitFor(); process.destroy(); }
        }
    }
    
    public static void main(String[] args) throws Exception {
        System.out.println(repeat("=", 95));
        System.out.println("Real-World UDF Benchmark: Embedded vs Cross-Process (Binary vs Arrow)");
        System.out.println(repeat("=", 95));
        System.out.println();
        
        initPython();
        System.out.println("Python initialized.\n");
        
        int batchSize = 10_000;
        int numBatches = 10;
        int iterations = 2;
        
        System.out.println("Config: " + batchSize + " rows/batch x " + numBatches + " batches = " 
            + String.format("%,d", batchSize * numBatches) + " rows\n");
        
        // Generate test data
        Random random = new Random(42);
        String[][] textBatches = new String[numBatches][batchSize];
        double[][] numBatches2 = new double[numBatches][batchSize];
        
        String[] samples = {
            "This product is amazing! Best purchase ever!",
            "Terrible quality, waste of money.",
            "北京市海淀区中关村大街1号",
            "110101199001011234",
            "2024-01-10 ERROR [UserService] Login failed"
        };
        
        for (int b = 0; b < numBatches; b++) {
            for (int i = 0; i < batchSize; i++) {
                textBatches[b][i] = samples[random.nextInt(samples.length)];
                numBatches2[b][i] = random.nextDouble() * 1000;
            }
        }
        
        // UDF scenarios - all 10
        String[][] scenarios = {
            {"sentiment", "NLP Sentiment",
             "lambda texts, nums: [1.0 if any(w in t.lower() for w in ['amazing','love','best']) else (-1.0 if any(w in t.lower() for w in ['terrible','worst','bad']) else 0.0) for t in texts]"},
            {"geo_parse", "Address Parse",
             "lambda texts, nums: [{'province': t[:3] if len(t)>3 else t} for t in texts]"},
            {"ml_infer", "ML Inference",
             "lambda texts, nums: [1.0/(1.0+__import__('math').exp(-(len(t)*0.1+n*0.01))) for t,n in zip(texts,nums)]"},
            {"regex", "Regex Extract",
             "lambda texts, nums: [__import__('re').findall(r'(ERROR|WARN|INFO|\\d{4}-\\d{2}-\\d{2})', t) for t in texts]"},
            {"anomaly", "Anomaly Detect",
             "lambda texts, nums: (lambda m,s: [abs(n-m)>2*s for n in nums])(__import__('numpy').mean(nums),__import__('numpy').std(nums))"},
            {"mask_pii", "Data Masking",
             "lambda texts, nums: [t[:6]+'****'+t[-4:] if len(t)==18 and t.isdigit() else t for t in texts]"},
            {"pricing", "Pricing Rules",
             "lambda texts, nums: [n*(0.9 if n>500 else 0.95 if n>200 else 1.0) for n in nums]"},
            {"img_hash", "Image Hash",
             "lambda texts, nums: [__import__('hashlib').md5(t.encode()).hexdigest()[:16] for t in texts]"},
            {"ip_lookup", "IP Lookup",
             "lambda texts, nums: [{'country':'CN' if t[0].isdigit() else 'US'} for t in texts]"},
            {"ner", "NER Extract",
             "lambda texts, nums: [[(w,'PERSON') if w[0].isupper() and len(w)<=3 else (w,'OTHER') for w in t.split()[:3]] for t in texts]"},
        };
        
        System.out.println(repeat("-", 95));
        System.out.printf("%-12s %-16s %12s %12s %12s %10s %10s%n", 
            "Scenario", "Description", "Embedded", "Binary", "Arrow", "vs Binary", "vs Arrow");
        System.out.println(repeat("-", 95));
        
        for (String[] scenario : scenarios) {
            String name = scenario[0];
            String desc = scenario[1];
            String udf = scenario[2];
            
            double embeddedMs = 0, binaryMs = 0, arrowMs = 0;
            
            // Embedded
            for (int i = 0; i < 2; i++) benchmarkEmbeddedGenericUDF(textBatches[0], numBatches2[0], udf, 1);
            long start = System.nanoTime();
            for (int iter = 0; iter < iterations; iter++) {
                for (int b = 0; b < numBatches; b++) {
                    benchmarkEmbeddedGenericUDF(textBatches[b], numBatches2[b], udf, 1);
                }
            }
            embeddedMs = (System.nanoTime() - start) / 1_000_000.0 / iterations;
            
            // Cross-process with Binary
            try (PythonWorkerConnection conn = new PythonWorkerConnection(false)) {
                conn.setUDF(udf);
                conn.executeUDF(textBatches[0], numBatches2[0]); // warm up
                
                start = System.nanoTime();
                for (int iter = 0; iter < iterations; iter++) {
                    for (int b = 0; b < numBatches; b++) {
                        conn.executeUDF(textBatches[b], numBatches2[b]);
                    }
                }
                binaryMs = (System.nanoTime() - start) / 1_000_000.0 / iterations;
            }
            
            // Cross-process with Arrow
            try (PythonWorkerConnection conn = new PythonWorkerConnection(true)) {
                conn.setUDF(udf);
                conn.executeUDF(textBatches[0], numBatches2[0]); // warm up
                
                start = System.nanoTime();
                for (int iter = 0; iter < iterations; iter++) {
                    for (int b = 0; b < numBatches; b++) {
                        conn.executeUDF(textBatches[b], numBatches2[b]);
                    }
                }
                arrowMs = (System.nanoTime() - start) / 1_000_000.0 / iterations;
            }
            
            double vsBinary = binaryMs / embeddedMs;
            double vsArrow = arrowMs / embeddedMs;
            
            System.out.printf("%-12s %-16s %10.1fms %10.1fms %10.1fms %9.1fx %9.1fx%n",
                name, desc, embeddedMs, binaryMs, arrowMs, vsBinary, vsArrow);
        }
        
        System.out.println(repeat("-", 95));
        finalizePython();
        
        System.out.println();
        System.out.println("Columns:");
        System.out.println("  Embedded:  JNI embedded Python (no IPC)");
        System.out.println("  Binary:    Cross-process with simple binary serialization");
        System.out.println("  Arrow:     Cross-process with Arrow IPC (real Spark behavior)");
        System.out.println("  vs Binary: Binary time / Embedded time");
        System.out.println("  vs Arrow:  Arrow time / Embedded time (real Spark speedup)");
    }
    
    private static String repeat(String s, int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) sb.append(s);
        return sb.toString();
    }
}
