package top.soaringlab.MTCICEP.generator;

import org.apache.flink.cep.PatternSelectFunction;
import top.soaringlab.MTCICEP.event.IntervalEvent;
import top.soaringlab.MTCICEP.event.RawEvent;

import java.util.List;
import java.util.Map;

/**
 * @author rillusory
 * @Description
 * @date 1/26/24 1:46 PM
 **/
public class HeterogeneousIntervalElementsCollector<W extends IntervalEvent> implements PatternSelectFunction<RawEvent, W> {

    private Class<W> out;
    private String outValueDescription;


    public HeterogeneousIntervalElementsCollector(Class<W> out) {
        this.out = out;

    }

    @Override
    public W select(Map<String, List<RawEvent>> map) throws Exception {


        RawEvent startEvent, endEvent;

        startEvent = map.get("start").get(0);
        endEvent = map.get("end").get(0);


        long start, end;
        start = startEvent.getTimestamp();
        end = endEvent.getTimestamp();

        return out.getDeclaredConstructor( long.class, long.class, double.class, String.class, String.class).newInstance(start, end, 0, "", startEvent.getKey());

    }
}