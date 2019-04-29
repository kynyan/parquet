package homework.writer;

import homework.schema.FileMetadata;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@AllArgsConstructor
@Getter
public class Dataset {
    private FileMetadata fileMetadata;
    private List<PartitionChunk> partitions;
}
