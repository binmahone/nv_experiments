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
 * Batch Size Impact on Complex UDFs: Embedded vs Cross-Process
 */
public class BatchSizeComplexBenchmark {

    static {
        try { Class.forName("EmbeddedPythonUDF"); } 
        catch (ClassNotFoundException e) { throw new RuntimeException(e); }
    }

    private static final int TOTAL_ROWS = 100000;
    private static final int[] BATCH_SIZES = {1000, 10000, 100000};

    private static Process pythonProcess;
    private static Socket socket;
    private static DataInputStream socketIn;
    private static DataOutputStream socketOut;
    private static BufferAllocator allocator;

    public static void main(String[] args) throws Exception {
        System.out.println("\n" + repeat("=", 100));
        System.out.println("Batch Size Impact on Complex UDFs: Embedded vs Cross-Process");
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

        // Test UDFs with different complexity
        String[][] udfs = {
            {"simple", "Simple Multiply",
             "lambda texts, nums: [n * 1.1 for t, n in zip(texts, nums)]"},
            {"regex", "Regex Extract",
             "lambda texts, nums: [__import__('re').sub(r'[0-9]+', str(int(n)), t) " +
             "for t, n in zip(texts, nums)]"},
            {"anomaly", "Anomaly Detect (NumPy)",
             "lambda texts, nums: (lambda m,s: [abs(n-m)>2*s for n in nums])" +
             "(__import__('numpy').mean(nums),__import__('numpy').std(nums))"},
            {"ml_infer", "ML Inference",
             "lambda texts, nums: [len(t) * 0.1 + n * 0.01 for t, n in zip(texts, nums)]"},
        };

        System.out.println("\nTotal rows: " + TOTAL_ROWS);

        for (String[] udfInfo : udfs) {
            String name = udfInfo[0];
            String desc = udfInfo[1];
            String udf = udfInfo[2];

            System.out.println("\n" + repeat("-", 100));
            System.out.println("UDF: " + desc);
            System.out.println(repeat("-", 100));
            System.out.printf("%-12s %8s %12s %12s %10s %12s %12s %10s%n",
                             "Batch Size", "Batches", "Embedded", "Cross-Proc", "Speedup",
                             "Embed/row", "Cross/row", "IPC%");
            System.out.println(repeat("-", 100));

            for (int batchSize : BATCH_SIZES) {
                int numBatches = TOTAL_ROWS / batchSize;
                
                startPythonWorker();

                // Benchmark Embedded
                long embeddedStart = System.nanoTime();
                for (int b = 0; b < numBatches; b++) {
                    int offset = b * batchSize;
                    String[] batchStrings = Arrays.copyOfRange(allStrings, offset, offset + batchSize);
                    double[] batchDoubles = Arrays.copyOfRange(allDoubles, offset, offset + batchSize);
                    EmbeddedPythonUDF.benchmarkEmbeddedGenericUDF(batchStrings, batchDoubles, udf, 1);
                }
                long embeddedTime = System.nanoTime() - embeddedStart;

                // Benchmark Cross-Process
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
                double embedPerRow = embeddedTime / 1e3 / TOTAL_ROWS;  // us per row
                double crossPerRow = crossTime / 1e3 / TOTAL_ROWS;
                
                // Estimate IPC overhead percentage
                // IPC overhead = (crossTime - embeddedTime) / crossTime
                double ipcPct = 100.0 * (crossTime - embeddedTime) / crossTime;

                System.out.printf("%-12d %8d %10.1fms %10.1fms %9.1fx %10.2fus %10.2fus %9.0f%%%n",
                                 batchSize, numBatches, 
                                 embeddedTime / 1e6, crossTime / 1e6, speedup,
                                 embedPerRow, crossPerRow, ipcPct);
            }
        }

        System.out.println("\n" + repeat("=", 100));
        System.out.println("Summary:");
        System.out.println("  - IPC%: Percentage of cross-process time spent on IPC (not UDF computation)");
        System.out.println("  - Higher IPC% = more benefit from embedded Python");
        System.out.println("  - Complex UDFs have lower IPC% (UDF computation dominates)");
        System.out.println(repeat("=", 100));

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
