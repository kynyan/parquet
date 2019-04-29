package homework.writer;

import homework.schema.FileMetadata;
import homework.schema.Schema;

import java.io.IOException;

public interface PartitionWriter {
    void write(Object rec) throws IOException;

    void write(FileMetadata metadata) throws IOException;
}
