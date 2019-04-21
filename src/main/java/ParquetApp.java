import homework.writer.Generator;
import homework.writer.ParquetWriter;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SpringBootApplication
public class ParquetApp {
    public static void main(String[] args) {
        try (InputStream input = new FileInputStream("application.properties")) {
            Properties properties = new Properties();
            // load a properties file
            properties.load(input);
            BlockingQueue<Object> queue = new ArrayBlockingQueue<Object>(1024);
            Generator generator = new Generator(queue, 1024);
            generator.run();
            ExecutorService pool = Executors.newFixedThreadPool(4);
            pool.submit(new ParquetWriter(queue, properties));
        } catch (IOException ex) {
            ex.printStackTrace();
        }

    }
}
