package top.soaringlab.datasouce;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import top.soaringlab.oj.TankPressure;

import java.util.Calendar;
import java.util.Random;

public class ContainPressure implements SourceFunction<TankPressure> {
    boolean running = true;
    @Override
    public void run(SourceContext<TankPressure> sourceContext) throws Exception {
        Random random = new Random();
        Integer[] boxId= {0,1,2};
        double averagePressure = 16;

        while (running) {
            Integer bId = boxId[random.nextInt(boxId.length)];
            Double pressure = averagePressure * (random.nextDouble() * 0.15 + 0.95);
            Long timestamp = Calendar.getInstance().getTimeInMillis();
            sourceContext.collect(new TankPressure(bId, pressure, timestamp));
            Thread.sleep(200L);
        }


    }

    @Override
    public void cancel() {
        running = false;
    }
}
