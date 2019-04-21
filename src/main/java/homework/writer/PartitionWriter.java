package homework.writer;

import homework.schema.FileMetadata;

import java.io.IOException;

public interface PartitionWriter {
    void write(Partition rec) throws IOException;

    void write(FileMetadata metadata) throws IOException;
}
