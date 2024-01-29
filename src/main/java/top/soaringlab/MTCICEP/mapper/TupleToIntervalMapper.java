package top.soaringlab.MTCICEP.mapper;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import top.soaringlab.MTCICEP.event.IntervalEvent;

import java.sql.Timestamp;

/**
 * @author rillusory
 * @Description
 * @date 1/26/24 1:25 PM
 **/
public class TupleToIntervalMapper<W extends IntervalEvent> implements MapFunction<Tuple7<String, Timestamp, Timestamp, Double, String, Integer, String>, W> {

    private Class<W> out;
    public TupleToIntervalMapper(Class<W> out)
    {
        this.out = out;
    }
    @Override
    public W map(Tuple7<String, Timestamp, Timestamp, Double, String, Integer, String> tuple) throws Exception {
        return out.getDeclaredConstructor( long.class, long.class, double.class, String.class, String.class, String.class).newInstance( tuple.f1.toInstant().getEpochSecond() , tuple.f2.toInstant().getEpochSecond()
                , tuple.f3 , tuple.f4, tuple.f0, tuple.f6);
    }
}
