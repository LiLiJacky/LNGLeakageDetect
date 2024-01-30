package top.soaringlab.example.event;

import top.soaringlab.MTCICEP.event.IntervalEvent;

/**
 * @author rillusory
 * @Description
 * @date 1/26/24 5:03 PM
 **/
public class AggregateInterval extends IntervalEvent {
    public AggregateInterval(long sts, long ets, double value, String valueDescriptor, String key, String tc) {
        super(sts, ets, value, valueDescriptor, key, tc);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AggregateInterval) {
            AggregateInterval other = (AggregateInterval) obj;
            return getValue() == other.getValue() && this.getStartTimestamp() == other.getStartTimestamp()
                    && this.getEndTimestamp() == other.getEndTimestamp();
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Double.hashCode(getValue());
    }

    @Override
    public String toString()
    {
        return super.toString().replace("IntervalEvent","AggregateInterval");
    }
}
