package homework.writer;

import homework.model.City;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;

@Slf4j
public class Generator implements Runnable {
    private final static int POISON_PILL = -1;
    private BlockingQueue<Object> queue;
    private int numberToGenerate;
    private int queueSizeInBytes;

    public Generator(BlockingQueue<Object> queue, int numberToGenerate) {
        this.queue = queue;
        this.numberToGenerate = numberToGenerate;
    }

    @Override
    public void run() {
        for (int i = 0; i < numberToGenerate; i++) {
            queue.add(City.random());
        }
        queue.add(POISON_PILL);
    }
}
