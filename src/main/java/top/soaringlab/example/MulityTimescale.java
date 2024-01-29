package top.soaringlab.example;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import top.soaringlab.MTCICEP.condition.*;
import top.soaringlab.MTCICEP.generator.HomogeneousIntervalGenerator;
import top.soaringlab.example.event.TemperatureEvent;
import top.soaringlab.example.event.TemperatureWarning;
import top.soaringlab.example.event.ThresholdInterval;
import top.soaringlab.example.mapper.ThroughputRecorder;
import top.soaringlab.example.source.TemperatureSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * @author rillusory
 * @Description
 * @date 1/29/24 9:04 PM
 **/
public class MulityTimescale {
    public static void main(String[] args) throws Exception {
        testHomogenousIntervals();

    }

    private static void testHeterogenousIntervals() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<TemperatureEvent> myTemps = new ArrayList<>();

        myTemps.add(new TemperatureEvent("1", 1, 30, "ms"));
        myTemps.add(new TemperatureEvent("1", 2, 35, "ms"));
        myTemps.add(new TemperatureEvent("1", 3, 33, "ms"));
        myTemps.add(new TemperatureEvent("1", 4, 40, "ms"));

        DataStream<TemperatureEvent> inputEventStream = env.fromCollection(myTemps);

        HomogeneousIntervalGenerator<TemperatureEvent, TemperatureWarning> newInterval = new HomogeneousIntervalGenerator<>();

        newInterval.source(inputEventStream)
                .sourceType(TemperatureEvent.class)
                .condition(new RelativeCondition().relativeLHS(Operand.Value).relativeOperator(Operator.GreaterThan).relativeRHS(Operand.Last).operator(Operator.GreaterThanEqual).RHS(30))
                //  .minOccurrences(2)
                .targetType(TemperatureWarning.class)
                .maxOccurrences(3)
                .within(Time.milliseconds(5))
                .outputValue(Operand.Max)
                .produceOnlyMaximalIntervals(true);

        DataStream<TemperatureWarning> warning1 = newInterval.runWithCEP();

        inputEventStream.print();

        warning1.print();
        env.execute("CEP Interval job");
    }

    private static void testHomogenousIntervals() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setAutoWatermarkInterval(200);

        DataStream<TemperatureEvent> randomStream = env.addSource(new TemperatureSource(1, 1, 35))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<TemperatureEvent>forBoundedOutOfOrderness(Duration.ofMillis(10))
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp()));
        randomStream = randomStream.map(new ThroughputRecorder());
//        randomStream.print();

        //Threshold
        HomogeneousIntervalGenerator<TemperatureEvent, ThresholdInterval> threshold = new HomogeneousIntervalGenerator<>();
        Operation operation1 = new OperandWrapper(Operand.Value);
        Operation operation2 = new SingleValue(31);
        Expression expression = new ConditionNew(operation1,Operator.GreaterThanEqual,operation2);
        threshold.source(randomStream)
                .sourceType(TemperatureEvent.class)
                .targetType(ThresholdInterval.class)
                .condition(new AbsoluteCondition(expression))
                .within(Time.milliseconds(10))
                .outputValue(Operand.Average)
                .produceOnlyMaximalIntervals(true);
        DataStream<ThresholdInterval> thresholdWarning = threshold.runWithCEP();
        thresholdWarning.print();
//
////

//        AbsoluteCondition absoluteCondition = new AbsoluteCondition();
//        absoluteCondition.operator(Operator.Absolute).RHS(new AbsoluteCondition().LHS(Operand.Value).operator(Operator.Minus).RHS(Operand.Min));
//        //Delta
//        HomogeneousIntervalGenerator<TemperatureEvent, DeltaInterval> delta = new HomogeneousIntervalGenerator<>();
//        delta.source(randomStream)
//                .sourceType(TemperatureEvent.class)
//                .targetType(DeltaInterval.class)
//                .condition(new RelativeCondition().LHS(true).operator(Operator.Equals).RHS(true).relativeLHS(absoluteCondition).relativeOperator(Operator.GreaterThanEqual).relativeRHS(0.01))
//                .minOccurrences(2)
//                .within(Time.milliseconds((100)))
//                .outputValue(Operand.Max)
//                .produceOnlyMaximalIntervals(true);
//
//        DataStream<DeltaInterval> deltaWarning = delta.runWithCEP();
//        deltaWarning.print();
//
//
//        HomogeneousIntervalGenerator<TemperatureEvent, AggregateInterval> aggregate = new HomogeneousIntervalGenerator<>();
//
//        aggregate.source(randomStream)
//                .sourceType(TemperatureEvent.class)
//                .targetType(AggregateInterval.class)
//                .condition(new RelativeCondition().LHS(true).operator(Operator.Equals).RHS(true).relativeLHS(Operand.Average).relativeOperator(Operator.GreaterThan).relativeRHS(36))
//                .minOccurrences(2)
//                .within(Time.milliseconds((100)))
//                .outputValue(Operand.Average)
//                .produceOnlyMaximalIntervals(true);
//
//        DataStream<AggregateInterval> aggregateWarning = aggregate.runWithCEP();
//        aggregateWarning.print();

        //Step No 5
        //Trigger the programme execution by calling execute(), mode of execution (local or cluster).
        JobExecutionResult result = env.execute("CEP Interval job");
        //result.getAccumulatorResult("throughput");


    }
}
