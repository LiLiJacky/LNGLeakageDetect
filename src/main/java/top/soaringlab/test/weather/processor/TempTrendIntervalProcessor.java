package top.soaringlab.test.weather.processor;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import top.soaringlab.test.weather.event.TempDifInterval;
import top.soaringlab.test.weather.event.TempTrendInterval;
import top.soaringlab.test.weather.event.TemperatureEvent;
import top.soaringlab.test.weather.math.MannKendall;

import java.util.ArrayList;
import java.util.List;

import static top.soaringlab.test.weather.util.Common.dayToMillseconds;

/**
 * @author rillusory
 * @Description
 * @date 5/8/24 7:25 PM
 **/
public class TempTrendIntervalProcessor extends ProcessWindowFunction<TemperatureEvent, TempTrendInterval, String, TimeWindow> {


    @Override
    public void open(Configuration parameters) throws Exception {

    }

    @Override
    public void process(String s, ProcessWindowFunction<TemperatureEvent, TempTrendInterval, String, TimeWindow>.Context context, Iterable<TemperatureEvent> iterable, Collector<TempTrendInterval> collector) throws Exception {
        // 过期 map 中的过期开始时间戳数据
        int index = 0;
        List<Double> list = new ArrayList<>();
        Long startTime = 0L;
        for (TemperatureEvent event : iterable) {
            if (index == 0) startTime = event.getTimestamp();
            if (event.getValue() == 0) continue;
            if (event.getTimestamp() > startTime + dayToMillseconds(25) && event.getTimestamp() % dayToMillseconds(1) == 0) {
                double z = new MannKendall(list).calculateZ();
                if (z > 3.0) {
                    TempTrendInterval tempDifInterval = new TempTrendInterval(startTime, event.getTimestamp(), z, "TempTrendInterval", s, "day");
                    collector.collect(tempDifInterval);
                }
            }
            list.add(event.getValue());
            index += 1;
        }
    }
}
