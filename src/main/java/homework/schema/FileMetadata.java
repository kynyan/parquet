package homework.schema;

import lombok.Getter;

import java.util.Set;

@Getter
public class FileMetadata {
    private long timestamp;
    private int numberOfPartitions;
    private int numberOfElements;
    private Set<SchemaElement> schemaElements;

    public FileMetadata(int numberOfPartitions, long timestamp, Set<SchemaElement> schemaElements) {
        this.numberOfPartitions = numberOfPartitions;
        this.timestamp = timestamp;
        this.numberOfElements = schemaElements.size();
        this.schemaElements = schemaElements;
    }
}
