package top.soaringlab.MTCICEP.generator;

import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import top.soaringlab.MTCICEP.condition.*;
import top.soaringlab.MTCICEP.event.IntervalStatistics;
import top.soaringlab.MTCICEP.event.RawEvent;

/**
 * @author rillusory
 * @Description
 * @date 1/26/24 1:31 PM
 **/
public class RelativeIterativeCondition<S extends RawEvent> extends IterativeCondition<S> {

    public enum ConditionContainer {
        Until,
        Where
    }

    private ConditionContainer container;
    private static final long serialVersionUID = 2392863109523984059L;
    //private boolean intervalEntryMatched = false;
    private Condition condition;
    private ConditionEvaluator<S> conditionEvaluator;
    private IntervalStatistics stats = new IntervalStatistics();
    public RelativeIterativeCondition(Condition cond, ConditionContainer container) {
        conditionEvaluator = new CustomConditionEvaluator<>();
        condition = cond;
        this.container = container;
    }

    private boolean evaluateCondition(AbsoluteCondition condition, S s) throws Exception {

        boolean result = conditionEvaluator.evaluateCondition(condition, s);

        if (container == ConditionContainer.Until) {
            return !result;
        } else
            return result;
    }


    private boolean evaluateRelativeCondition(RelativeCondition condition, Iterable<S> prevMatches, S s) throws Exception {
        boolean result = conditionEvaluator.evaluateRelativeCondition(condition,prevMatches, s);
        if (container == ConditionContainer.Until)
            return !result;
        else
            return result;
    }

    private boolean evaluateRelativeCondition(RelativeCondition condition, S s) throws Exception {
        boolean result = conditionEvaluator.evaluateRelativeCondition(condition,stats, s);
        if (container == ConditionContainer.Until)
            return !result;
        else
            return result;
    }

    @Override
    public boolean filter(S s, Context<S> context) throws Exception {
        if (condition instanceof AbsoluteCondition)
            return evaluateCondition((AbsoluteCondition) condition, s);
        else {
            Iterable<S> items = context.getEventsForPattern("1");
            if (!items.iterator().hasNext()) {
                stats.reset();
                stats.first = stats.last = stats.min = stats.max = s.getValue();
                stats.sum = s.getValue();
                stats.count = 1;
                return evaluateCondition(((RelativeCondition) condition).getStartCondition(), s);
            } else // there are previous items
            {
                Boolean result = evaluateRelativeCondition((RelativeCondition) condition, items, s);
                if (result)
                {
                    stats.last = s.getValue();
                    stats.min = Math.min(stats.min, s.getValue());
                    stats.max = Math.max(stats.max, s.getValue());
                    stats.sum+=s.getValue();
                    stats.count++;
                }
                return  result;
            }
        }


    }
}

