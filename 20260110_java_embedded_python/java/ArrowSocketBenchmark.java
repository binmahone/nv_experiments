import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.*;

/**
 * Benchmark comparing:
 * 1. Embedded Python (JNI) - zero IPC overhead
 * 2. Cross-process with real Arrow IPC over Socket (like real Spark)
 * 
 * This uses:
 * - Real Apache Arrow Java Library for serialization (like Spark)
 * - TCP Socket for IPC (like real Spark Python worker)
 * - pyarrow on Python side for deserialization
 */
public class ArrowSocketBenchmark {

    // Initialize EmbeddedPythonUDF to load the native library
    static {
        // Force EmbeddedPythonUDF class to load, which loads the native library
        try {
            Class.forName("EmbeddedPythonUDF");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("EmbeddedPythonUDF class not found", e);
        }
    }

    private static final int BATCH_SIZE = 10000;  // Spark default maxRecordsPerBatch
    private static final int NUM_BATCHES = 10;

    // Socket connection to Python worker
    private static Process pythonProcess;
    private static Socket socket;
    private static DataInputStream socketIn;
    private static DataOutputStream socketOut;
    private static BufferAllocator allocator;

    public static void main(String[] args) throws Exception {
        System.out.println("\n" + repeat("=", 95));
        System.out.println("Real-World UDF Benchmark: Embedded vs Cross-Process (Arrow over Socket)");
        System.out.println(repeat("=", 95));

        // Initialize
        allocator = new RootAllocator();
        EmbeddedPythonUDF.initPython();
        System.out.println("\nPython initialized (embedded).");

        // Start Python worker with socket server
        startPythonWorker();

        System.out.println("\nConfig: " + BATCH_SIZE + " rows/batch x " + NUM_BATCHES + 
                          " batches = " + (BATCH_SIZE * NUM_BATCHES) + " rows");

        // Prepare test data
        double[] doubleData = new double[BATCH_SIZE];
        String[] stringData = new String[BATCH_SIZE];
        Random rand = new Random(42);
        for (int i = 0; i < BATCH_SIZE; i++) {
            doubleData[i] = rand.nextDouble() * 1000;
            stringData[i] = "text_" + i + "_data";
        }

        // Real-world UDF scenarios (mixed types)
        String[][] scenarios = {
            {"sentiment", "NLP Sentiment",
             "lambda texts, nums: [1 if 'good' in t or n > 500 else -1 if 'bad' in t else 0 " +
             "for t, n in zip(texts, nums)]"},
            {"geo_parse", "Address Parse",
             "lambda texts, nums: [t.split('_')[0] + '_' + str(int(n)) for t, n in zip(texts, nums)]"},
            {"ml_infer", "ML Inference",
             "lambda texts, nums: [len(t) * 0.1 + n * 0.01 for t, n in zip(texts, nums)]"},
            {"regex", "Regex Extract",
             "lambda texts, nums: [__import__('re').sub(r'[0-9]+', str(int(n)), t) " +
             "for t, n in zip(texts, nums)]"},
            {"anomaly", "Anomaly Detect",
             "lambda texts, nums: (lambda m,s: [abs(n-m)>2*s for n in nums])" +
             "(__import__('numpy').mean(nums),__import__('numpy').std(nums))"},
            {"mask_pii", "Data Masking",
             "lambda texts, nums: [t[:4] + '***' + str(int(n))[-2:] for t, n in zip(texts, nums)]"},
            {"pricing", "Pricing Rules",
             "lambda texts, nums: [n * 1.1 if 'premium' in t else n * 0.9 " +
             "for t, n in zip(texts, nums)]"},
            {"img_hash", "Image Hash",
             "lambda texts, nums: [hash(t + str(n)) % 1000000 for t, n in zip(texts, nums)]"},
            {"ip_lookup", "IP Lookup",
             "lambda texts, nums: [t.replace('text', str(int(n) % 256)) for t, n in zip(texts, nums)]"},
            {"ner", "NER Extract",
             "lambda texts, nums: [{'text': t, 'score': n/1000} for t, n in zip(texts, nums)]"},
        };

        System.out.println("\n" + repeat("-", 95));
        System.out.printf("%-12s %-20s %12s %12s %12s%n",
                         "Scenario", "Description", "Embedded", "Arrow+Socket", "Speedup");
        System.out.println(repeat("-", 95));

        for (String[] scenario : scenarios) {
            String name = scenario[0];
            String desc = scenario[1];
            String udf = scenario[2];

            // Benchmark embedded (uses JNI, zero IPC)
            long embeddedTime = EmbeddedPythonUDF.benchmarkEmbeddedGenericUDF(
                stringData, doubleData, udf, NUM_BATCHES);

            // Benchmark cross-process with real Arrow over Socket
            long arrowSocketTime = benchmarkArrowSocket(stringData, doubleData, udf, NUM_BATCHES);

            double speedup = (double) arrowSocketTime / embeddedTime;

            System.out.printf("%-12s %-20s %10.1fms %10.1fms %10.1fx%n",
                             name, desc, embeddedTime / 1e6, arrowSocketTime / 1e6, speedup);
        }

        System.out.println(repeat("-", 95));
        System.out.println("\nColumns:");
        System.out.println("  Embedded:      JNI embedded Python (no IPC, zero-copy for numerics)");
        System.out.println("  Arrow+Socket:  Cross-process with Java Arrow Library + TCP Socket");
        System.out.println("  Speedup:       Arrow+Socket time / Embedded time");
        System.out.println("\nThis benchmark uses:");
        System.out.println("  - Real Apache Arrow Java Library (arrow-vector 15.0.0) for serialization");
        System.out.println("  - TCP Socket for IPC (like real Spark Python workers)");
        System.out.println("  - pyarrow on Python side for Arrow IPC deserialization");

        // Cleanup
        stopPythonWorker();
        allocator.close();
    }

    private static void startPythonWorker() throws Exception {
        // Start Python socket server
        ProcessBuilder pb = new ProcessBuilder("python3", "python_socket_worker.py");
        pb.directory(new File("."));
        pb.redirectErrorStream(false);
        pythonProcess = pb.start();

        // Read the port from Python worker's stdout
        BufferedReader reader = new BufferedReader(
            new InputStreamReader(pythonProcess.getInputStream()));
        String line = reader.readLine();
        if (line == null || !line.startsWith("PORT:")) {
            // Read stderr for error message
            BufferedReader errReader = new BufferedReader(
                new InputStreamReader(pythonProcess.getErrorStream()));
            String errLine;
            while ((errLine = errReader.readLine()) != null) {
                System.err.println("Python error: " + errLine);
            }
            throw new RuntimeException("Failed to start Python worker: " + line);
        }
        int port = Integer.parseInt(line.substring(5).trim());

        // Connect to Python worker
        Thread.sleep(100);  // Give Python time to start listening
        socket = new Socket("localhost", port);
        socket.setTcpNoDelay(true);  // Disable Nagle's algorithm for lower latency
        socketIn = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        socketOut = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        
        System.out.println("Connected to Python worker on port " + port + " (TCP Socket)");
    }

    private static void stopPythonWorker() throws Exception {
        if (socket != null && !socket.isClosed()) {
            try {
                // Send shutdown signal
                socketOut.writeShort(8);  // Length of "SHUTDOWN"
                socketOut.writeBytes("SHUTDOWN");
                socketOut.flush();
            } catch (Exception e) {
                // Ignore errors during shutdown
            }
            socket.close();
        }
        if (pythonProcess != null) {
            pythonProcess.waitFor(5, TimeUnit.SECONDS);
            pythonProcess.destroyForcibly();
        }
    }

    /**
     * Benchmark cross-process UDF execution with:
     * - Java Arrow Library for serialization (ArrowStreamWriter)
     * - TCP Socket for IPC
     * - pyarrow on Python side for deserialization
     */
    private static long benchmarkArrowSocket(String[] stringData, double[] doubleData,
                                             String udfCode, int iterations) throws Exception {
        long totalTime = 0;

        for (int iter = 0; iter < iterations; iter++) {
            long start = System.nanoTime();

            // === STEP 1: Send command (EXECUTE + UDF code) ===
            byte[] udfBytes = udfCode.getBytes(StandardCharsets.UTF_8);
            socketOut.writeShort(7);  // Length of "EXECUTE"
            socketOut.writeBytes("EXECUTE");
            socketOut.writeShort(udfBytes.length);
            socketOut.write(udfBytes);

            // === STEP 2: Serialize data using Java Arrow Library ===
            Schema schema = new Schema(Arrays.asList(
                Field.nullable("texts", new ArrowType.Utf8()),
                Field.nullable("nums", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))
            ));

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                VarCharVector textVector = (VarCharVector) root.getVector("texts");
                Float8Vector numVector = (Float8Vector) root.getVector("nums");

                // Calculate total byte size for string data
                int totalByteSize = 0;
                for (String s : stringData) {
                    totalByteSize += s.getBytes(StandardCharsets.UTF_8).length;
                }

                // Allocate vectors with proper size
                textVector.allocateNew(totalByteSize, stringData.length);
                numVector.allocateNew(stringData.length);

                // Fill vectors with data
                for (int i = 0; i < stringData.length; i++) {
                    byte[] bytes = stringData[i].getBytes(StandardCharsets.UTF_8);
                    textVector.set(i, bytes);
                    numVector.set(i, doubleData[i]);
                }
                textVector.setValueCount(stringData.length);
                numVector.setValueCount(doubleData.length);
                root.setRowCount(stringData.length);

                // Serialize to Arrow IPC stream format
                try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, baos)) {
                    writer.start();
                    writer.writeBatch();
                    writer.end();
                }
            }

            byte[] arrowData = baos.toByteArray();

            // === STEP 3: Send Arrow data over Socket ===
            socketOut.writeInt(arrowData.length);
            socketOut.write(arrowData);
            socketOut.flush();

            // === STEP 4: Read response ===
            int responseSize = socketIn.readInt();
            byte[] responseData = new byte[responseSize];
            socketIn.readFully(responseData);

            // === STEP 5: Deserialize Arrow response ===
            try (ArrowStreamReader reader = new ArrowStreamReader(
                    new ByteArrayInputStream(responseData), allocator)) {
                reader.loadNextBatch();
                // Result available in reader.getVectorSchemaRoot()
            }

            totalTime += System.nanoTime() - start;
        }

        return totalTime;
    }

    // Helper to repeat string for Java 8 compatibility
    private static String repeat(String s, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(s);
        }
        return sb.toString();
    }
}
