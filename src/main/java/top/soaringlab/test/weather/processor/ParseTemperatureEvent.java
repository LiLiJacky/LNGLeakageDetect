package top.soaringlab.test.weather.processor;

/**
 * @author rillusory
 * @Description
 * @date 4/23/24 12:03 AM
 **/
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import top.soaringlab.test.weather.event.TemperatureEvent;
import top.soaringlab.test.weather.oj.TemperatureLog;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static top.soaringlab.test.weather.util.Common.convertStringToLong;

public class ParseTemperatureEvent implements FlatMapFunction<TemperatureLog, TemperatureEvent> {


    @Override
    public void flatMap(TemperatureLog tempLog, Collector<TemperatureEvent> out) throws Exception {
        long timestamp = convertStringToLong(tempLog.getDatetime());

        String[] cities = tempLog.getCities();
        for (int i = 1; i <= cities.length; i++) {
            double temp = tempLog.getTemperatureByName(cities[i - 1]);
            TemperatureEvent temperatureEvent = new TemperatureEvent(timestamp, cities[i - 1], temp);
            out.collect(temperatureEvent);
        }
    }
}

