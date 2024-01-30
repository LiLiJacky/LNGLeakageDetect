package top.soaringlab.MTCICEP.event;

import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.StringValue;

/**
 * @author rillusory
 * @Description
 * @date 1/26/24 1:02 PM
 **/
public class IntervalEvent  extends Tuple6<LongValue, LongValue, DoubleValue, StringValue, StringValue, StringValue> {

//    protected long startTimestamp;
//    protected long endTimestamp;
//    protected double value;
//    protected String valueDescriptor;

//    protected String key
//    protected String timeScale
//      protected String intervalName

    public IntervalEvent()
    {
        this(Long.MIN_VALUE, Long.MAX_VALUE, -1, "NONE", "dummy", "ms");
    }
    public IntervalEvent(long sts, long ets, double value, String valueDescriptor, String k, String tc)
    {

        this.f0 = new LongValue(sts);
        this.f1 = new LongValue(ets);
        this.f2 = new DoubleValue(value);
        this.f3 = new StringValue(valueDescriptor);
        this.f4 = new StringValue(k);
        this.f5 = new StringValue(tc);
    }

    public long getStartTimestamp(){return f0.getValue();}

    public long getEndTimestamp(){return f1.getValue();}

    public double getValue(){ return f2.getValue();}

    public String getKey(){ return f4.getValue(); }

    public String getValueDescriptor(){return f3.getValue();}

    public String getTimeScale(){return f5.getValue();}


    @Override
    public String toString()
    {
        return "IntervalEvent(start: " + getStartTimestamp() + ", end: " + getEndTimestamp() + ", value: " + getValue() +
                ", value description: "+ getValueDescriptor() +", key: " + getKey() + ", time scale: " + getTimeScale() +")";
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof  IntervalEvent))
            return false;
        IntervalEvent otherInterval = (IntervalEvent) other;
        return this.getStartTimestamp() == otherInterval.getStartTimestamp() && this.getEndTimestamp() == otherInterval.getEndTimestamp()
                && this.getValue() == otherInterval.getValue() && this.getValueDescriptor().equals(otherInterval.getValueDescriptor())
                && this.getKey().equals(otherInterval.getKey()) && this.getTimeScale().equals(otherInterval.getEndTimestamp());
    }

}
