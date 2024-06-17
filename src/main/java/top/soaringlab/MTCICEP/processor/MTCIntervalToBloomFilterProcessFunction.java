package top.soaringlab.MTCICEP.processor;

import com.espertech.esper.common.internal.collection.TimeWindow;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;
import scala.Int;
import top.soaringlab.MTCICEP.bloomfilter.BloomFilter;
import top.soaringlab.MTCICEP.condition.AggregationWindowSize;
import top.soaringlab.MTCICEP.event.EventRecord;
import top.soaringlab.MTCICEP.event.IntervalEvent;

/**
 * @author rillusory
 * @Description
 * @date 1/31/24 1:51 PM
 **/
public class MTCIntervalToBloomFilterProcessFunction<I extends IntervalEvent, E extends EventRecord> extends KeyedProcessFunction<Tuple, I, E> {
    private BloomFilter bloomFilter;
    private Class<I> in;
    private Class<E> out;
    private AggregationWindowSize aggregationWindowSize;
    ValueState<Long> windowEdge;
    ValueState<Integer> windowEventNums;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Long> windowEdgeDescriptor = new ValueStateDescriptor<>("windowEdge", Types.LONG);
        windowEdge = getRuntimeContext().getState(windowEdgeDescriptor);
        ValueStateDescriptor<Integer> windowEventNumsDescriptor = new ValueStateDescriptor<>("windowEventNums", Types.INT);
        windowEventNums = getRuntimeContext().getState(windowEventNumsDescriptor);
    }

    public MTCIntervalToBloomFilterProcessFunction(BloomFilter<String> bf, Class<I> in, Class<E> out) {
        this.bloomFilter = bf;
        this.in = in;
        this.out = out;
    }


    public void storageElement(I i) {
        StringBuilder nowEvent = new StringBuilder();
        nowEvent.append(i.getKey());
        nowEvent.append(i.getClass().getName());
        Long startTime = i.getStartTimestamp();
        Long endTime = i.getEndTimestamp();
        Long fitrtEdge = aggregationWindowSize.getRightEdge(startTime);
        int times = 0;
        while (fitrtEdge <= endTime || times == 0) {
            String nowElement = nowEvent.toString() + String.valueOf(fitrtEdge);
            bloomFilter.add(nowElement);
            fitrtEdge += aggregationWindowSize.getWindowSize();
            times += 1;
        }

    }

    @Override
    public void processElement(I i, KeyedProcessFunction<Tuple, I, E>.Context context, Collector<E> collector) throws Exception {
        
        if (this.aggregationWindowSize == null) {
            String tc = i.getTimeScale();
            if (tc.equals("ms")) {
                aggregationWindowSize = AggregationWindowSize.ms;
            } else if (tc.equals("seconds")) {
                aggregationWindowSize = AggregationWindowSize.seconds;
            } else if (tc.equals("minutes")) {
                aggregationWindowSize = AggregationWindowSize.minutes;
            } else if (tc.equals("hours")) {
                aggregationWindowSize = AggregationWindowSize.hours;
            } else if (tc.equals("days")) {
                aggregationWindowSize = AggregationWindowSize.days;
            } else if (tc.equals("month")) {
                aggregationWindowSize = AggregationWindowSize.month;
            } else if (tc.equals("years")) {
                aggregationWindowSize = AggregationWindowSize.years;
            } else throw new IllegalArgumentException();
        }
        Long timeEdge = windowEdge.value();

        if (timeEdge == null) {
            windowEdge.update(aggregationWindowSize.getRightEdge(i.getEndTimestamp()) + aggregationWindowSize.getAggregationBFSize());
        }
        System.out.println(i);

        this.storageElement(i);

        if (i.getEndTimestamp() >= windowEdge.value()) {
            int count = bloomFilter.count();
            String bv = bloomFilter.bitToString();
            Long ts = aggregationWindowSize.getRightEdge(i.getEndTimestamp());
            String tc = i.getTimeScale();
            String key = i.getKey();
            collector.collect((E) new EventRecord(bv, count, ts, tc, key));
            bloomFilter.clear();
            windowEdge.update(aggregationWindowSize.getRightEdge(i.getEndTimestamp()) + aggregationWindowSize.getAggregationBFSize());
            windowEventNums.update(0);
            this.storageElement(i);
        }
    }
}
