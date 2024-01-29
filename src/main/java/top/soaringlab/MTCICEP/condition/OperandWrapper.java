package top.soaringlab.MTCICEP.condition;

/**
 * @author rillusory
 * @Description
 * @date 1/27/24 6:35 PM
 **/
public class OperandWrapper extends Operation{

    private Operand operand;

    public OperandWrapper(Operand operand) {
        this.operand = operand;
    }

    @Override
    public Operation first() {
        throw new UnsupportedOperationException("Operand Wrapper does not have a first parameter.");
    }

    @Override
    public Operation second() {
        return this;
    }

    @Override
    public double calculate(double first, double last, double min, double max, double sum, int count, double currentValue, double avg) {
        //eval method takes the aggregate as parameters, thus returns the aggregate correspondant to the operand
        switch(operand){
            case Average: return avg;
            case Sum: return sum;
            case Max: return max;
            case Min: return min;
            case First: return first;
            case Last: return last;
            case Value: return currentValue;
            default: throw new IllegalArgumentException();
        }
    }
}
