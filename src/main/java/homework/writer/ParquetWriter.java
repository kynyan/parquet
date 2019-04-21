package homework.writer;

import homework.schema.FileMetadata;
import homework.schema.SchemaElement;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.BlockingQueue;

@Slf4j
public class ParquetWriter implements Runnable {
    private final static String THREADS_NUMBER = "threads.number";
    private final static String DIRECTORY = "parquet.directory";
    private final static String DATASET_SIZE = "dataset.size";
    private int datasetSize = 100;
    private String directory = "/Users/nik/Desktop/parquet/";
    private int parallelism = 4;
    private PartitionWriter partitionWriter;

    private final static int POISON_PILL = -1;
    private final static Map<String, Integer> TYPES = new HashMap<>();
    static {
        TYPES.put("int", 0);
        TYPES.put("long", 1);
        TYPES.put("double", 2);
        TYPES.put("float", 3);
        TYPES.put("String", 4);
    }

    private BlockingQueue<Object> queue;

    public ParquetWriter(BlockingQueue<Object> queue, Properties properties) {
        this.queue = queue;
        this.datasetSize = Integer.valueOf(properties.getProperty(DATASET_SIZE));
        this.directory = properties.getProperty(DIRECTORY);
        this.parallelism = Integer.valueOf(properties.getProperty(THREADS_NUMBER));
        this.partitionWriter = new LocalSystemWriter(directory);
    }

    public ParquetWriter() {
        this.partitionWriter = new LocalSystemWriter(directory);
    }

    public void writeParquet(Object[] data) throws NoSuchFieldException, IOException {
        Set<SchemaElement> schemaElements = getSchemaElements(data[0]);
        long timestamp = System.currentTimeMillis();
        List<Partition> partitions = getPartitions(timestamp, schemaElements, data);
        FileMetadata fileMetadata = new FileMetadata(partitions.size(), timestamp, schemaElements);
        Dataset dataset = new Dataset(fileMetadata, partitions);
        write(dataset);
        write(fileMetadata);
    }

    private void write(Dataset dataset) throws IOException {
        for (Partition partition : dataset.getPartitions()) {
            partitionWriter.write(partition);
        }
    }

    private void write(FileMetadata metadata) throws IOException {
        partitionWriter.write(metadata);
    }

    private List<Partition> getPartitions(long timestamp, Set<SchemaElement> schema, Object[] generatedData) throws NoSuchFieldException {
        List<Partition> partitions = new ArrayList<>();
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
            Partition partition = new Partition(timestamp, element.getType(), element.getName(), columnValues);
            partitions.add(partition);
        }
        return partitions;
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
    public void run() {
        while(true) {
            log.info("Parquet writer " + Thread.currentThread().getName()+" START");
            try {
                List<Object> data = new ArrayList<>(datasetSize);
                queue.drainTo(data, datasetSize);
                writeParquet(data.toArray());
                if (data.contains(POISON_PILL)) break;
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("Consumer "+Thread.currentThread().getName()+" END");
        }
    }
}
