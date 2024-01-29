package top.soaringlab.MTCICEP.event;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.StringValue;

/**
 * @author rillusory
 * @Description
 * @date 1/26/24 12:37 PM
 **/
public abstract class RawEvent extends Tuple4<StringValue, LongValue, DoubleValue, StringValue> {// implements Serializable {

//    protected long timestamp;
//    protected double value;
//    protected String key;
//    protected String timesScale;

    public RawEvent()
    {
        f0 = new StringValue("dummy");
        f1 = new LongValue(Long.MIN_VALUE);
        f2 = new DoubleValue(-1d);
        f3 = new StringValue("ms");

    }
    public RawEvent(long ts, double v) {
        this("dummy", ts, v, "ms");
    }
    protected RawEvent(String k, long ts, double v)
    {
        this.f1 = new LongValue(ts);
        this.f0 = new StringValue(k);
        this.f2 = new DoubleValue(v);
        this.f3 = new StringValue("ms");
    }
    protected RawEvent(String k, long ts, double v, String tc)
    {
        this.f1 = new LongValue(ts);
        this.f0 = new StringValue(k);
        this.f2 = new DoubleValue(v);
        this.f3 = new StringValue(tc);
    }

    public long getTimestamp(){return f1.getValue();}

    public String getKey() { return f0.getValue(); }

    public double getValue() {return f2.getValue();}

    public String getTimeScale() {return f3.getValue();}

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RawEvent)) return false;
        RawEvent other = (RawEvent) obj;

        return other.getTimestamp() == this.getTimestamp() && other.getValue() == this.getValue()
                && other.getKey().equals(this.getKey()) && other.getTimeScale().equals(this.getTimeScale());
    }
}

