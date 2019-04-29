package homework.writer;

import homework.schema.FileMetadata;
import homework.schema.SchemaElement;
import lombok.extern.slf4j.Slf4j;
import org.ehcache.sizeof.SizeOf;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class LocalSystemWriter implements PartitionWriter {
    private static final int CHUNK_SIZE = 100;
    static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS");
    private BlockingQueue<Object> queue;
    private ExecutorService pool;
    private LocalDateTime partitionStart;
    private String partitionDirectory;
    private Set<SchemaElement> schemaElements;
    //configurable parameters
    private String directory; //path to folder where all data will be written to
    private int parallelism = 4; //number of writer threads
    private int bufferMemoryLimit = 1024; //in Mb
    private int partitionTimeInterval = 1; //in min

    private final static Map<String, Integer> TYPES = new HashMap<>();
    static {
        TYPES.put("int", 0);
        TYPES.put("long", 1);
        TYPES.put("double", 2);
        TYPES.put("float", 3);
        TYPES.put("String", 4);
    }

    public LocalSystemWriter(String directory) {
        this.directory = directory;
        this.pool = Executors.newFixedThreadPool(parallelism);
    }

    public LocalSystemWriter(String directory, int numberOfWriterThreads, int bufferMemoryLimit, int partitionTimeInterval) {
        this.directory = directory;
        this.parallelism = numberOfWriterThreads;
        this.bufferMemoryLimit = bufferMemoryLimit;
        this.partitionTimeInterval = partitionTimeInterval;
        this.pool = Executors.newFixedThreadPool(parallelism);
    }

    @Override
    public void write(Object rec) {
        //using the first object, calculate it's size, and number of elements for the queue
        //create queue
        //accept objects, put them in a queue
        //start consumption
        if (queue == null) {
            queue = getQueueWithLimitedMemory(rec);
            partitionStart = LocalDateTime.now();
            partitionDirectory = String.format("%s%s", directory, partitionStart);
            schemaElements = getSchemaElements(rec);
        }
        try {
            queue.put(rec);
        } catch (InterruptedException e) {
            log.error("Interrupted exception", e.getCause());
        }
        partitionDirectory = getPartitionDirectoryTimely();
        pool.submit(() -> {
            new PartitionChunkWriter(queue, CHUNK_SIZE, partitionDirectory, schemaElements).run();
            log.info("{} thread completed writing partition chunk to {}", Thread.currentThread().getName(), partitionDirectory);
        });
    }

    private String getPartitionDirectoryTimely() {
        LocalDateTime partitionEnd = partitionStart.plusMinutes(partitionTimeInterval);
        if (!partitionStart.isBefore(partitionEnd)) {
            partitionStart = partitionEnd;
        }
        return String.format("%s%s", directory, partitionStart);
    }

    private ArrayBlockingQueue<Object> getQueueWithLimitedMemory(Object rec) {
        SizeOf sizeOf = SizeOf.newInstance();
        Long sizeInBytes = sizeOf.sizeOf(rec);
        int queueSize = (bufferMemoryLimit * 1024 * 1024) / sizeInBytes.intValue();
        return new ArrayBlockingQueue<>(queueSize);
    }

    private Set<SchemaElement> getSchemaElements(Object object) {
        Comparator<SchemaElement> comp = Comparator.comparingInt(SchemaElement::getType);
        Set<SchemaElement> elements = new TreeSet<>(comp);
        Field[] fields = object.getClass().getDeclaredFields();
        for (Field f : fields) {
            String type = f.getType().getSimpleName();
            if (!type.equals("List")) {
                elements.add(new SchemaElement(TYPES.get(type), f.getName()));
            }
        }
        return elements;
    }

    @Override
    public void write(FileMetadata metadata) throws IOException {
        String filename = String.format("%s/file_metadata", partitionDirectory);
        RandomAccessFile writer = new RandomAccessFile(filename, "rw");
        byte[] bytesToWrite = getFileMetadataBytes(metadata);
        writer.write(bytesToWrite);
        writer.close();
    }

    private byte[] getFileMetadataBytes(FileMetadata metadata) {
        ByteBuffer buffer = ByteBuffer.allocate(512);
        buffer.putLong(metadata.getTimestamp());
        buffer.putInt(metadata.getNumberOfPartitions());
        buffer.putInt(metadata.getNumberOfElements());
        for (SchemaElement element : metadata.getSchemaElements()) {
            buffer.putInt(element.getType());
            buffer.putInt(element.getName().length());
            buffer.put(element.getName().getBytes());
        }
        return buffer.array();
    }


}
