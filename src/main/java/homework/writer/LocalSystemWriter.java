package homework.writer;

import homework.schema.FileMetadata;
import homework.schema.SchemaElement;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@Slf4j
public class LocalSystemWriter implements PartitionWriter {
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS");
    private String path;

    LocalSystemWriter(String directory) {
        String partitionStart = FORMATTER.format(LocalDateTime.now(ZoneId.of("UTC")));
        this.path = String.format("%s%s", directory, partitionStart);
    }

    @Override
    public void write(Partition rec) throws IOException {
        String filename = makeSureDirectoryExistsAndGetFilename();
        RandomAccessFile writer = new RandomAccessFile(filename, "rw");
        byte[] bytesToWrite = getPartitionBytes(rec);
        writer.write(bytesToWrite);
        writer.close();
    }

    @Override
    public void write(FileMetadata metadata) throws IOException {
        String filename = String.format("%s/file_metadata", path);
        RandomAccessFile writer = new RandomAccessFile(filename, "rw");
        byte[] bytesToWrite = getFileMetadataBytes(metadata);
        writer.write(bytesToWrite);
        writer.close();
    }

    private byte[] getPartitionBytes(Partition rec) {
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

    private String makeSureDirectoryExistsAndGetFilename() {
        File dir = new File(path);
        if (!dir.exists()) dir.mkdirs();
        String partitionStart = FORMATTER.format(LocalDateTime.now(ZoneId.of("UTC")));
        return String.format("%s/%s", path, partitionStart);
    }
}
