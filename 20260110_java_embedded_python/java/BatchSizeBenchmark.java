import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.*;

/**
 * Benchmark: How batch size affects Embedded vs Cross-Process performance
 * 
 * Hypothesis: Embedded Python doesn't need small batches because there's no IPC overhead.
 */
public class BatchSizeBenchmark {

    static {
        try {
            Class.forName("EmbeddedPythonUDF");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static final int TOTAL_ROWS = 100000;  // Total data size
    
    // Test different batch sizes
    private static final int[] BATCH_SIZES = {1000, 5000, 10000, 50000, 100000};

    private static Process pythonProcess;
    private static Socket socket;
    private static DataInputStream socketIn;
    private static DataOutputStream socketOut;
    private static BufferAllocator allocator;

    public static void main(String[] args) throws Exception {
        System.out.println("\n" + repeat("=", 90));
        System.out.println("Batch Size Impact: Embedded vs Cross-Process");
        System.out.println(repeat("=", 90));

        allocator = new RootAllocator();
        EmbeddedPythonUDF.initPython();

        // Prepare test data
        double[] allDoubles = new double[TOTAL_ROWS];
        String[] allStrings = new String[TOTAL_ROWS];
        Random rand = new Random(42);
        for (int i = 0; i < TOTAL_ROWS; i++) {
            allDoubles[i] = rand.nextDouble() * 1000;
            allStrings[i] = "text_" + i;
        }

        // Simple UDF for fair comparison
        String udf = "lambda texts, nums: [n * 1.1 for t, n in zip(texts, nums)]";

        System.out.println("\nTotal rows: " + TOTAL_ROWS);
        System.out.println("UDF: " + udf);

        System.out.println("\n" + repeat("-", 90));
        System.out.printf("%-12s %10s %12s %12s %12s %10s%n",
                         "Batch Size", "Batches", "Embedded", "Cross-Proc", "Speedup", "Note");
        System.out.println(repeat("-", 90));

        for (int batchSize : BATCH_SIZES) {
            int numBatches = TOTAL_ROWS / batchSize;
            
            // Start fresh Python worker for each batch size test
            startPythonWorker();

            // ===== Benchmark Embedded =====
            long embeddedStart = System.nanoTime();
            for (int b = 0; b < numBatches; b++) {
                int offset = b * batchSize;
                String[] batchStrings = Arrays.copyOfRange(allStrings, offset, offset + batchSize);
                double[] batchDoubles = Arrays.copyOfRange(allDoubles, offset, offset + batchSize);
                EmbeddedPythonUDF.benchmarkEmbeddedGenericUDF(batchStrings, batchDoubles, udf, 1);
            }
            long embeddedTime = System.nanoTime() - embeddedStart;

            // ===== Benchmark Cross-Process =====
            long crossStart = System.nanoTime();
            for (int b = 0; b < numBatches; b++) {
                int offset = b * batchSize;
                String[] batchStrings = Arrays.copyOfRange(allStrings, offset, offset + batchSize);
                double[] batchDoubles = Arrays.copyOfRange(allDoubles, offset, offset + batchSize);
                benchmarkArrowSocket(batchStrings, batchDoubles, udf);
            }
            long crossTime = System.nanoTime() - crossStart;

            stopPythonWorker();

            double speedup = (double) crossTime / embeddedTime;
            String note = "";
            if (batchSize == 10000) note = "<- Spark default";
            if (batchSize == 100000) note = "<- No batching";

            System.out.printf("%-12d %10d %10.1fms %10.1fms %10.1fx %10s%n",
                             batchSize, numBatches, 
                             embeddedTime / 1e6, crossTime / 1e6, speedup, note);
        }

        System.out.println(repeat("-", 90));

        System.out.println("\nKey Insights:");
        System.out.println("  - Cross-process: More batches = more IPC overhead = slower");
        System.out.println("  - Embedded: Batch size has minimal impact (no IPC)");
        System.out.println("  - Embedded can process entire partition at once!");

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
        // Send command
        byte[] udfBytes = udfCode.getBytes(StandardCharsets.UTF_8);
        socketOut.writeShort(7);
        socketOut.writeBytes("EXECUTE");
        socketOut.writeShort(udfBytes.length);
        socketOut.write(udfBytes);

        // Create Arrow schema and data
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

        // Read response
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
