package top.soaringlab.MTCICEP.condition;

import java.io.Serializable;

/**
 * @author rillusory
 * @Description
 * @date 1/26/24 1:10 PM
 **/
public abstract class Condition implements Serializable {
    protected Object lhs;
    protected Object rhs;

    protected Operator operator;
    private Expression internalExpression;

    public Condition() {
    }

    protected Condition(Expression internalExpression) {
        this.internalExpression = internalExpression;
    }

    public Expression getInternalExpression() {
        return internalExpression;
    }

    public abstract Condition LHS(Object operand);

    public abstract Condition RHS(Object operand);

    public abstract Condition operator(Operator op);

    public String toString() {
        if (lhs == null)
        {
            lhs = Operand.Value;
        }
        return lhs.toString() + operator.toString() + rhs.toString();
    }


    public Object getRHS()
    {
        return rhs;
    }

    public Operator getOperator()
    {
        return operator;
    }
}
