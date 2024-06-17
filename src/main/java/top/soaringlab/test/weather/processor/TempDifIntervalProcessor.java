package top.soaringlab.test.weather.processor;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import top.soaringlab.test.weather.event.TempDifInterval;
import top.soaringlab.test.weather.event.TemperatureEvent;

import java.util.*;

/**
 * @author rillusory
 * @Description
 * @date 5/8/24 7:25 PM
 **/
public class TempDifIntervalProcessor extends ProcessWindowFunction<TemperatureEvent, TempDifInterval, String, TimeWindow> {
    private transient ValueState<Integer> isProcessed;
    private transient MapState<Long, Long> matchedTimestamps;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Integer> isProcessedDes = new ValueStateDescriptor<>(
                "firstNIsProcessed",
                Integer.class,
                0
        );
        isProcessed = getRuntimeContext().getState(isProcessedDes);

        MapStateDescriptor<Long, Long> matchDesc = new MapStateDescriptor<>(
                "processed-timestamps", // state name
                BasicTypeInfo.LONG_TYPE_INFO, // key type
                BasicTypeInfo.LONG_TYPE_INFO // last timestamp has been already processed
        );
        matchedTimestamps = getRuntimeContext().getMapState(matchDesc);
    }

    @Override
    public void process(String s, ProcessWindowFunction<TemperatureEvent, TempDifInterval, String, TimeWindow>.Context context, Iterable<TemperatureEvent> iterable, Collector<TempDifInterval> collector) throws Exception {
        // 过期 map 中的过期开始时间戳数据
        for (Long beginTime : matchedTimestamps.keys()) {
            if (beginTime < context.currentProcessingTime()) matchedTimestamps.remove(beginTime);
        }

        int index = 0;
        List<TemperatureEvent> list = new ArrayList<>();
        for (TemperatureEvent event : iterable) {
            if (event.getValue() == 0) continue;
            list.add(event);
        }

        //if (isProcessed.value()) index = 5;
        while (index < list.size()) {
            long beginTime = list.get(index).getTimestamp();
            int distance = 1;
            while (index + distance < list.size()) {
                long endTime = list.get(index + distance).getTimestamp();
                if (endTime - beginTime > 5 * 24 * 60 * 60 * 1000) break;
                if (endTime - beginTime < 24 * 60 * 60 * 1000) {
                    distance += 1;
                    continue;
                }
                double temd = list.get(index + distance).getValue() - list.get(index).getValue();
                if (temd < -20) {
                    // 去重
                    if (matchedTimestamps.contains(beginTime)) {
                        long lastTimeStamp = matchedTimestamps.get(beginTime);
                        if (endTime <= lastTimeStamp) {
                            distance += 1;
                            continue;
                        }
                        else {
                            // 判断是否是自然天
                            if (endTime - beginTime % 24 * 60 * 60 * 1000 != 0) {
                                int now = isProcessed.value();
                                now += 1;
                                isProcessed.update(now);
                                System.out.println(now);
                            }

                        }
                    }
                    //matchedTimestamps.put(beginTime, endTime);
                    TempDifInterval tempDifInterval = new TempDifInterval(list.get(index).getTimestamp(), list.get(index + distance).getTimestamp(), temd, "TempDifInterval", s, "hour");
                    collector.collect(tempDifInterval);
                }
                distance += 1;
            }
            index += 1;
        }
    }
}
