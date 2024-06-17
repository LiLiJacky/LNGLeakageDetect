package top.soaringlab.test.weather.processor;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import top.soaringlab.test.weather.event.ColdWaveInterval;
import top.soaringlab.test.weather.event.TempTrendInterval;
import top.soaringlab.test.weather.event.TemperatureEvent;
import top.soaringlab.test.weather.math.MannKendall;
import top.soaringlab.test.weather.math.TempDiff;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static top.soaringlab.test.weather.util.Common.dayToMillseconds;

/**
 * @author rillusory
 * @Description
 * @date 5/8/24 7:25 PM
 **/
public class TempTrendIntervalAndDiffProcessor extends ProcessWindowFunction<TemperatureEvent, ColdWaveInterval, String, TimeWindow> {


    @Override
    public void open(Configuration parameters) throws Exception {

    }

    @Override
    public void process(String s, ProcessWindowFunction<TemperatureEvent, ColdWaveInterval, String, TimeWindow>.Context context, Iterable<TemperatureEvent> iterable, Collector<ColdWaveInterval> collector) throws Exception {
        // 过期 map 中的过期开始时间戳数据
        int index = 0;
        List<Double> temp_list = new ArrayList<>();
        List<Long> ts_list = new ArrayList<>();
        Long startTime = 0L;
        for (TemperatureEvent event : iterable) {
            if (index == 0) startTime = event.getTimestamp();
            if (event.getValue() == 0) continue;
            if (event.getTimestamp() > startTime + dayToMillseconds(25) && event.getTimestamp() % dayToMillseconds(1) == 0) {
                double z = new MannKendall(temp_list).calculateZ();
                if (z > 3.0) {
                    ColdWaveInterval coldWaveInterval = new ColdWaveInterval(startTime, event.getTimestamp(), z, "ColdWaveInterval", s, "day");
                    boolean eval = new TempDiff().eval(temp_list, ts_list);
                    if (eval) collector.collect(coldWaveInterval);
                }
            }
            temp_list.add(event.getValue());
            ts_list.add(event.getTimestamp());
            index += 1;
        }
    }
}
