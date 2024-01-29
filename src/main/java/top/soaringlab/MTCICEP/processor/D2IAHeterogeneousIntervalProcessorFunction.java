package top.soaringlab.MTCICEP.processor;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import top.soaringlab.MTCICEP.event.IntervalEvent;
import top.soaringlab.MTCICEP.event.RawEvent;

/**
 * @author rillusory
 * @Description
 * @date 1/26/24 1:52 PM
 **/
public class D2IAHeterogeneousIntervalProcessorFunction<S extends RawEvent, E extends RawEvent, F extends RawEvent, I extends IntervalEvent> extends ProcessWindowFunction<RawEvent, I, String, GlobalWindow> {
    @Override
    public void process(String s, Context context, Iterable<RawEvent> iterable, Collector<I> collector) throws Exception {

    }
}