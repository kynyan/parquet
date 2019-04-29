package homework.writer;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class PartitionChunk {
    private long timestamp; //link to parquet file metadata
    private int type;
    private String name;
    private Object[] values;

    public PartitionChunk(int chunkSize) {
        values = new Object[chunkSize];
    }
}
