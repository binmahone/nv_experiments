import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.*;

/**
 * Compare:
 * - Cross-process: Spark default 10,000 rows/batch (must use small batches)
 * - Embedded: 100,000 rows at once (no batch limit!)
 */
public class LargeBatchBenchmark {

    static {
        try { Class.forName("EmbeddedPythonUDF"); } 
        catch (ClassNotFoundException e) { throw new RuntimeException(e); }
    }

    private static final int TOTAL_ROWS = 100000;
    private static final int SPARK_BATCH_SIZE = 10000;  // Spark default

    private static Process pythonProcess;
    private static Socket socket;
    private static DataInputStream socketIn;
    private static DataOutputStream socketOut;
    private static BufferAllocator allocator;

    public static void main(String[] args) throws Exception {
        System.out.println("\n" + repeat("=", 100));
        System.out.println("Real-World Comparison: Spark Default (10k batch) vs Embedded (No Batch Limit)");
        System.out.println(repeat("=", 100));

        allocator = new RootAllocator();
        EmbeddedPythonUDF.initPython();

        // Prepare test data
        double[] allDoubles = new double[TOTAL_ROWS];
        String[] allStrings = new String[TOTAL_ROWS];
        Random rand = new Random(42);
        for (int i = 0; i < TOTAL_ROWS; i++) {
            allDoubles[i] = rand.nextDouble() * 1000;
            allStrings[i] = "text_" + i + "_data";
        }

        // 10 Real-world UDF scenarios
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

        System.out.println("\nTotal rows: " + TOTAL_ROWS);
        System.out.println("Cross-process: " + SPARK_BATCH_SIZE + " rows/batch x " + 
                          (TOTAL_ROWS/SPARK_BATCH_SIZE) + " batches (Spark default)");
        System.out.println("Embedded: " + TOTAL_ROWS + " rows at once (no batch limit)");

        System.out.println("\n" + repeat("-", 100));
        System.out.printf("%-12s %-20s %15s %15s %12s%n",
                         "Scenario", "Description", "Cross (10k×10)", "Embed (100k×1)", "Speedup");
        System.out.println(repeat("-", 100));

        double totalCross = 0, totalEmbed = 0;

        for (String[] scenario : scenarios) {
            String name = scenario[0];
            String desc = scenario[1];
            String udf = scenario[2];

            startPythonWorker();

            // Cross-process: 10,000 rows × 10 batches (Spark default)
            long crossStart = System.nanoTime();
            int numBatches = TOTAL_ROWS / SPARK_BATCH_SIZE;
            for (int b = 0; b < numBatches; b++) {
                int offset = b * SPARK_BATCH_SIZE;
                String[] batchStrings = Arrays.copyOfRange(allStrings, offset, offset + SPARK_BATCH_SIZE);
                double[] batchDoubles = Arrays.copyOfRange(allDoubles, offset, offset + SPARK_BATCH_SIZE);
                benchmarkArrowSocket(batchStrings, batchDoubles, udf);
            }
            long crossTime = System.nanoTime() - crossStart;

            stopPythonWorker();

            // Embedded: 100,000 rows at once (no batch limit)
            long embedStart = System.nanoTime();
            EmbeddedPythonUDF.benchmarkEmbeddedGenericUDF(allStrings, allDoubles, udf, 1);
            long embedTime = System.nanoTime() - embedStart;

            double speedup = (double) crossTime / embedTime;
            totalCross += crossTime;
            totalEmbed += embedTime;

            System.out.printf("%-12s %-20s %13.1fms %13.1fms %10.1fx%n",
                             name, desc, crossTime / 1e6, embedTime / 1e6, speedup);
        }

        System.out.println(repeat("-", 100));
        System.out.printf("%-12s %-20s %13.1fms %13.1fms %10.1fx%n",
                         "TOTAL", "All 10 UDFs", totalCross / 1e6, totalEmbed / 1e6, 
                         totalCross / totalEmbed);
        System.out.println(repeat("-", 100));

        System.out.println("\nKey Point:");
        System.out.println("  Cross-process MUST use small batches (memory, pipeline, etc.)");
        System.out.println("  Embedded CAN process entire partition at once (no such limits)");
        System.out.println("  This is the REAL speedup potential of embedded Python!");

        allocator.close();
    }

    private static void startPythonWorker() throws Exception {
        ProcessBuilder pb = new ProcessBuilder("python3", "python_socket_worker.py");
        pb.directory(new File("."));
        pythonProcess = pb.start();
        BufferedReader reader = new BufferedReader(
            new InputStreamReader(pythonProcess.getInputStream()));
        String line = reader.readLine();
        int port = Integer.parseInt(line.substring(5).trim());
        Thread.sleep(50);
        socket = new Socket("localhost", port);
        socket.setTcpNoDelay(true);
        socketIn = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        socketOut = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
    }

    private static void stopPythonWorker() throws Exception {
        if (socket != null && !socket.isClosed()) {
            try {
                socketOut.writeShort(8);
                socketOut.writeBytes("SHUTDOWN");
                socketOut.flush();
            } catch (Exception e) {}
            socket.close();
        }
        if (pythonProcess != null) {
            pythonProcess.destroyForcibly();
        }
    }

    private static void benchmarkArrowSocket(String[] stringData, double[] doubleData,
                                             String udfCode) throws Exception {
        byte[] udfBytes = udfCode.getBytes(StandardCharsets.UTF_8);
        socketOut.writeShort(7);
        socketOut.writeBytes("EXECUTE");
        socketOut.writeShort(udfBytes.length);
        socketOut.write(udfBytes);

        Schema schema = new Schema(Arrays.asList(
            Field.nullable("texts", new ArrowType.Utf8()),
            Field.nullable("nums", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))
        ));

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            VarCharVector textVector = (VarCharVector) root.getVector("texts");
            Float8Vector numVector = (Float8Vector) root.getVector("nums");

            int totalByteSize = 0;
            for (String s : stringData) {
                totalByteSize += s.getBytes(StandardCharsets.UTF_8).length;
            }
            textVector.allocateNew(totalByteSize, stringData.length);
            numVector.allocateNew(stringData.length);

            for (int i = 0; i < stringData.length; i++) {
                textVector.set(i, stringData[i].getBytes(StandardCharsets.UTF_8));
                numVector.set(i, doubleData[i]);
            }
            textVector.setValueCount(stringData.length);
            numVector.setValueCount(doubleData.length);
            root.setRowCount(stringData.length);

            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, baos)) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
        }

        byte[] arrowData = baos.toByteArray();
        socketOut.writeInt(arrowData.length);
        socketOut.write(arrowData);
        socketOut.flush();

        int responseSize = socketIn.readInt();
        byte[] responseData = new byte[responseSize];
        socketIn.readFully(responseData);
    }

    private static String repeat(String s, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) sb.append(s);
        return sb.toString();
    }
}
