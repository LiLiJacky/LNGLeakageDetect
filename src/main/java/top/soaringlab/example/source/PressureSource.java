package top.soaringlab.example.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import top.soaringlab.example.event.PressureEvent;
import top.soaringlab.example.event.PressureEvent;

import java.util.Random;

/**
 * @author rillusory
 * @Description
 * @date 1/26/24 4:52 PM
 **/
public class PressureSource implements SourceFunction<PressureEvent> {

    private boolean running = true;

    private final long pause;
    private final double pressureStd;
    private final double pressureMean;
    private Random random;

    public PressureSource(long pause, double pressureStd, double pressureMean) {
        this.pause = pause;
        this.pressureMean = pressureMean;
        this.pressureStd = pressureStd;
        random = new Random();
    }

    @Override
    public void run(SourceContext<PressureEvent> sourceContext) throws Exception {
        while (running) {
            String id = "1";
            PressureEvent pressureEvent;
            double pressure = random.nextGaussian() * pressureStd + pressureMean;
            long timestamp = System.currentTimeMillis();
            pressureEvent = new PressureEvent(id, timestamp, pressure, "seconds");
            sourceContext.collectWithTimestamp(pressureEvent, timestamp);
            Thread.sleep(pause*1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

}