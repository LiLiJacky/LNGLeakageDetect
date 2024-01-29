package top.soaringlab.example.event;

import top.soaringlab.MTCICEP.event.IntervalEvent;

/**
 * @author rillusory
 * @Description
 * @date 1/26/24 5:01 PM
 **/
public class ThresholdInterval extends IntervalEvent {

    public ThresholdInterval() {}
    public ThresholdInterval(long sts, long ets, double value, String valueDescriptor, String key, String tc) {
        super(sts, ets, value, valueDescriptor, key, tc);
    }

//    @Override
//    public long getStartTimestamp() {
//        return startTimestamp;
//    }
//
//    @Override
//    public long getEndTimestamp() {
//        return endTimestamp;
//    }
//
//    public double getValue() {
//        return value;
//    }


    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ThresholdInterval) {
            ThresholdInterval other = (ThresholdInterval) obj;
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
        return super.toString().replace("IntervalEvent","ThresholdInterval");
    }

}

