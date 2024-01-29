package top.soaringlab.MTCICEP.condition;

/**
 * @author rillusory
 * @Description
 * @date 1/27/24 6:36 PM
 **/
public class SingleValue extends Operation {

    private double value;

    public SingleValue(double value) {
        this.value = value;
    }

    @Override
    public Operation first() {
        throw new UnsupportedOperationException("Single Value Expression does not have the first member.");
    }

    @Override
    public Operation second() {
        return this;
    }

    @Override
    public double calculate(double first, double last, double min, double max, double sum, int count, double currentValue, double avg) {
        return value;
    }
}

