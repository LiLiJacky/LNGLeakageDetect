package top.soaringlab.example.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import top.soaringlab.example.event.TemperatureEvent;

import java.util.Random;

/**
 * @author rillusory
 * @Description
 * @date 1/26/24 4:52 PM
 **/
public class TemperatureSource implements SourceFunction<TemperatureEvent> {

    private boolean running = true;

    private final long pause;
    private final double temperatureStd;
    private final double temperatureMean;
    private Random random;

    public TemperatureSource(long pause, double temperatureStd, double temperatureMean) {
        this.pause = pause;
        this.temperatureMean = temperatureMean;
        this.temperatureStd = temperatureStd;
        random = new Random();
    }

//    @Override
//    public void open(Configuration configuration) {
//
//        random = new Random();
//    }

    @Override
    public void run(SourceFunction.SourceContext<TemperatureEvent> sourceContext) throws Exception {
        while (running) {
            String id = "1";
            TemperatureEvent temperatureEvent;
            double temperature = random.nextGaussian() * temperatureStd + temperatureMean;


//            DateFormat dateFormat = new SimpleDateFormat("yyyy");
//            Date dateFrom = dateFormat.parse("2012");
//            long timestampFrom = dateFrom.getTime();
//            Date dateTo = dateFormat.parse("2013");
//            long timestampTo = dateTo.getTime();
//            Random random = new Random();
//            long timeRange = timestampTo - timestampFrom;
//            long randomTimestamp = timestampFrom + (long) (random.nextDouble() * timeRange);
            long timestamp = System.currentTimeMillis();
            temperatureEvent = new TemperatureEvent(id, timestamp, temperature, "ms");
            //System.out.println(temperatureEvent);
            sourceContext.collectWithTimestamp(temperatureEvent, timestamp);
            Thread.sleep(pause);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

}