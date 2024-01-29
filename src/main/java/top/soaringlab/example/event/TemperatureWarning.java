package top.soaringlab.example.event;

import top.soaringlab.MTCICEP.event.IntervalEvent;

import java.io.Serializable;

/**
 * @author rillusory
 * @Description
 * @date 1/26/24 3:53 PM
 **/
public class TemperatureWarning extends IntervalEvent implements Serializable {


    public TemperatureWarning(long sts, long ets, double value, String valueDescriptor, String key, String tc) {
        super(sts, ets, value, valueDescriptor, key, tc);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TemperatureWarning) {
            TemperatureWarning other = (TemperatureWarning) obj;
            return getValue() == other.getValue() && this.getStartTimestamp() == other.getStartTimestamp()
                    && this.getEndTimestamp() == other.getEndTimestamp() && this.getKey().equals(other.getKey());
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
        return super.toString().replace("IntervalEvent","TemperatureWarning");
    }

}


