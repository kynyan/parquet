package homework;

import homework.model.City;
import homework.writer.ParquetWriter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.List;

@RunWith(SpringRunner.class)
public class ParquetFileTest {

    @Test
    public void shouldWriteFileMetadata() throws NoSuchFieldException, IOException {
        ParquetWriter parquetWriter = new ParquetWriter();
        List<City> cities = City.randomCities(10);
        parquetWriter.writeParquet(cities.toArray());
    }

//    @Test
//    public void shouldReadFileMetadata() throws IOException {
//        parquetWriter.generateData(10);
//        RandomAccessFile reader = new RandomAccessFile("/Users/nik/Desktop/parquet/20190415", "r");
//        int numberOfRows = reader.readInt();
//        int numberOfElements = reader.readInt();
//        assertEquals(100, numberOfRows);
//        assertEquals(4, numberOfElements);
//        List<String> fields = new ArrayList<>();
//        List<Integer> types = new ArrayList<>();
//        for (int i = 0; i < numberOfElements; i++) {
//            types.add(reader.readInt());
//            fields.add(getString(reader));
//        }
//        assertEquals("name", fields.get(0));
////        assertEquals(1, types.get(0));
//    }

//    private String getString(RandomAccessFile reader) throws IOException {
//        int length = reader.readInt();
//        byte[] bytes = new byte[length];
//        reader.read(bytes);
//        return new String(bytes);
//    }
//

}
