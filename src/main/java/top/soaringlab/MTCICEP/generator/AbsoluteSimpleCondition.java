package top.soaringlab.MTCICEP.generator;

import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import top.soaringlab.MTCICEP.condition.AbsoluteCondition;
import top.soaringlab.MTCICEP.condition.ConditionEvaluator;
import top.soaringlab.MTCICEP.condition.CustomConditionEvaluator;
import top.soaringlab.MTCICEP.event.RawEvent;

/**
 * @author rillusory
 * @Description
 * @date 1/26/24 1:45 PM
 **/
public class AbsoluteSimpleCondition<S extends RawEvent> extends SimpleCondition<S> {

    AbsoluteCondition condition;
    ConditionEvaluator conditionEvaluator;
    public AbsoluteSimpleCondition(AbsoluteCondition condition)
    {
        this.condition = condition;
        //conditionEvaluator = new JaninoConditionEvaluator<>();
        conditionEvaluator = new CustomConditionEvaluator<>();
    }
    @Override
    public boolean filter(S s) throws Exception {
        return conditionEvaluator.evaluateCondition(condition, s);
    }
}
