package top.soaringlab.MTCICEP.condition;

import java.io.Serializable;

public interface Expression extends Serializable {
    public Expression first();
    public Expression second();
    public boolean eval(double first, double last, double min, double max, double sum, int count, double currentValue, double avg);
    public double calculate(double first, double last, double min, double max, double sum, int count, double currentValue, double avg);
}
