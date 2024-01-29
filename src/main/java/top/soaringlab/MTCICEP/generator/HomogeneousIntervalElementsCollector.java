package top.soaringlab.MTCICEP.generator;

import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.util.Collector;
import top.soaringlab.MTCICEP.condition.Operand;
import top.soaringlab.MTCICEP.event.IntervalEvent;
import top.soaringlab.MTCICEP.event.RawEvent;

import java.util.List;
import java.util.Map;

/**
 * @author rillusory
 * @Description
 * @date 1/26/24 1:47 PM
 **/
class HomogeneousIntervalElementsCollector<S extends RawEvent, W extends IntervalEvent> implements PatternSelectFunction<S, W> {

    private Class<W> out;
    private Operand outValueOperand;
    private List<S> matchingEvents;
    public HomogeneousIntervalElementsCollector(Class<W> out, Operand outputValueOperand) {
        this.out = out;
        this.outValueOperand = outputValueOperand;
    }


    public void flatSelect(Map<String, List<S>> map, Collector<W> collector) throws Exception {
        System.out.println("not in");
        matchingEvents = map.get("1");
        for(S i : matchingEvents) {
            System.out.println(i.toString());
        }
        System.out.println("selecet");
        double outputValue = 0;
        String rid = matchingEvents.get(0).getKey();
        String outValueDescription = outValueOperand.toString();
        System.out.println(outValueDescription);
        if (outValueOperand == Operand.First) {
            outputValue = matchingEvents.get(0).getValue();

        } else if (outValueOperand == Operand.Last) {
            outputValue = matchingEvents.get(matchingEvents.size() - 1).getValue();
        } else         if (outValueOperand == Operand.Average) {
            for (S s : matchingEvents) {
                outputValue += s.getValue();
            }
            outputValue = outputValue / matchingEvents.size();
        } else         if (outValueOperand == Operand.Sum) {
            for (S s : matchingEvents) {
                outputValue += s.getValue();
            }
        } else if (outValueOperand == Operand.Max ){
            outputValue = matchingEvents.get(0).getValue();
            for (S s:matchingEvents)
            {
                outputValue = Double.max(outputValue, s.getValue());
            }
        } else if (outValueOperand == Operand.Min ) {
            outputValue = matchingEvents.get(0).getValue();
            for (S s : matchingEvents) {
                outputValue = Double.min(outputValue, s.getValue());
            }
        }
        long start, end;
        start = matchingEvents.get(0).getTimestamp();
        end = matchingEvents.get(matchingEvents.size()-1).getTimestamp();

        //return out.getDeclaredConstructor( long.class, long.class, double.class, String.class, String.class).newInstance(start, end, outputValue, outValueDescription, rid);

    }

    public W select(Map<String, List<S>> map) throws Exception {
        System.out.println("not in");
        matchingEvents = map.get("1");
        for(S i : matchingEvents) {
            System.out.println(i.toString());
        }
        System.out.println("selecet");
        double outputValue = 0;
        String rid = matchingEvents.get(0).getKey();
        String outValueDescription = outValueOperand.toString();
        System.out.println(outValueDescription);
        if (outValueOperand == Operand.First) {
            outputValue = matchingEvents.get(0).getValue();

        } else if (outValueOperand == Operand.Last) {
            outputValue = matchingEvents.get(matchingEvents.size() - 1).getValue();
        } else         if (outValueOperand == Operand.Average) {
            for (S s : matchingEvents) {
                outputValue += s.getValue();
            }
            outputValue = outputValue / matchingEvents.size();
        } else         if (outValueOperand == Operand.Sum) {
            for (S s : matchingEvents) {
                outputValue += s.getValue();
            }
        } else if (outValueOperand == Operand.Max ){
            outputValue = matchingEvents.get(0).getValue();
            for (S s:matchingEvents)
            {
                outputValue = Double.max(outputValue, s.getValue());
            }
        } else if (outValueOperand == Operand.Min ) {
            outputValue = matchingEvents.get(0).getValue();
            for (S s : matchingEvents) {
                outputValue = Double.min(outputValue, s.getValue());
            }
        }
        long start, end;
        start = matchingEvents.get(0).getTimestamp();
        end = matchingEvents.get(matchingEvents.size()-1).getTimestamp();
        String tc = matchingEvents.get(0).getTimeScale();

        return out.getDeclaredConstructor( long.class, long.class, double.class, String.class, String.class, String.class).newInstance(start, end, outputValue, outValueDescription, rid, tc);

    }
}

