package homework.writer;

import homework.schema.FileMetadata;
import homework.schema.SchemaElement;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import static homework.writer.LocalSystemWriter.FORMATTER;

@Slf4j
public class PartitionChunkWriter implements Runnable {
    private BlockingQueue<Object> queue;
    private int chunkSize;
    private List<PartitionChunk> chunks;
    private String partitionDirectory;
    private Set<SchemaElement> schemaElements;
    private final static int POISON_PILL = -1;

    PartitionChunkWriter(BlockingQueue<Object> queue, int chunkSize, String directory, Set<SchemaElement> schemaElements) {
        this.queue = queue;
        this.chunkSize = chunkSize;
        this.partitionDirectory = directory;
        this.schemaElements = schemaElements;
    }

    @Override
    public void run() {
        while(true) {
            log.info("Partition chunk writer {} start", Thread.currentThread().getName());
            try {
                List<Object> data = new ArrayList<>();
                while (data.size() < chunkSize) {
                    data.add(queue.take());
                }
                writePartitionChunk(data.toArray());
                if (data.contains(POISON_PILL)) break;
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("Consumer "+Thread.currentThread().getName()+" END");
        }
    }

    private void writePartitionChunk(Object[] data) throws NoSuchFieldException, IOException {
        long timestamp = System.currentTimeMillis();
        List<PartitionChunk> partitions = getPartitions(timestamp, schemaElements, data);
        FileMetadata fileMetadata = new FileMetadata(partitions.size(), timestamp, schemaElements);
        Dataset dataset = new Dataset(fileMetadata, partitions);
        write(dataset);
//        write(fileMetadata);
    }

    private void write(Dataset dataset) throws IOException {
        for (PartitionChunk partition : dataset.getPartitions()) {
            write(partition);
        }
    }

    public void write(PartitionChunk rec) throws IOException {
        String filename = makeSureDirectoryExistsAndGetFilename(rec);
        RandomAccessFile writer = new RandomAccessFile(filename, "rw");
        byte[] bytesToWrite = getPartitionBytes(rec);
        writer.write(bytesToWrite);
        writer.close();
    }

    private byte[] getPartitionBytes(PartitionChunk rec) {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.putLong(rec.getTimestamp());
        buffer.putInt(rec.getType());
        buffer.putInt(rec.getName().length());
        buffer.put(rec.getName().getBytes());
        buffer.putInt(rec.getValues().length);
        switch (rec.getType()) {
            case 0:
                for (Object value : rec.getValues()) buffer.putInt((int) value);
                break;
            case 1:
                for (Object value : rec.getValues()) buffer.putLong((long) value);
                break;
            case 2:
                for (Object value : rec.getValues()) buffer.putDouble((double) value);
                break;
            case 3:
                for (Object value : rec.getValues()) buffer.putFloat((float) value);
                break;
            case 4:
                for (Object value : rec.getValues()) buffer.put(value.toString().getBytes());
                break;
        }
        return buffer.array();
    }

    private String makeSureDirectoryExistsAndGetFilename(PartitionChunk rec) {
        File dir = new File(partitionDirectory);
        if (!dir.exists()) dir.mkdirs();
        String partitionStart = FORMATTER.format(LocalDateTime.now(ZoneId.of("UTC")));
        return String.format("%s/%s_%s_%s", partitionDirectory, partitionStart, rec.getType(), rec.getName());
    }

    private List<PartitionChunk> getPartitions(long timestamp, Set<SchemaElement> schema, Object[] generatedData) throws NoSuchFieldException {
        List<PartitionChunk> partitions = new ArrayList<>();
        for (SchemaElement element : schema) {
            Object[] columnValues = new Object[generatedData.length];
            for (int i = 0; i < generatedData.length; i++) {
                try {
                    Field field = generatedData[i].getClass().getDeclaredField(element.getName());
                    field.setAccessible(true);
                    columnValues[i] = field.get(generatedData[i]);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
            PartitionChunk partition = new PartitionChunk(timestamp, element.getType(), element.getName(), columnValues);
            partitions.add(partition);
        }
        return partitions;
    }
}
