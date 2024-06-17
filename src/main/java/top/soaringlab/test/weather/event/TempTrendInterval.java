package top.soaringlab.test.weather.event;

import top.soaringlab.MTCICEP.event.IntervalEvent;

/**
 * @author rillusory
 * @Description
 * @date 5/8/24 7:17 PM
 **/
public class TempTrendInterval extends IntervalEvent {
    public TempTrendInterval() {
        super();
    }

    public TempTrendInterval(long sts, long ets, double value, String valueDescriptor, String k, String tc) {
        super(sts, ets, value, valueDescriptor, k, tc);
    }


    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TempTrendInterval) {
            TempTrendInterval other = (TempTrendInterval) obj;
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
        return super.toString().replace("IntervalEvent","TemTrendIntervalEvent");
    }
}
