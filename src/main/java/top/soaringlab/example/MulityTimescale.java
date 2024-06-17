package top.soaringlab.example;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import top.soaringlab.MTCICEP.condition.*;
import top.soaringlab.MTCICEP.event.EventRecord;
import top.soaringlab.MTCICEP.generator.EventRecordGenerator;
import top.soaringlab.MTCICEP.generator.HomogeneousIntervalGenerator;
import top.soaringlab.MTCICEP.generator.IntervalOperator;
import top.soaringlab.MTCICEP.generator.Match;
import top.soaringlab.example.event.*;
import top.soaringlab.example.mapper.PressureEventRecorder;
import top.soaringlab.example.mapper.TemperatureEventRecorder;
import top.soaringlab.example.source.PressureSource;
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

    private static void testHomogenousIntervals() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setAutoWatermarkInterval(200);

        // millisStream
        DataStream<TemperatureEvent> millisStream = env.addSource(new TemperatureSource(1, 1, 35))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<TemperatureEvent>forBoundedOutOfOrderness(Duration.ofMillis(10))
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp()));
        millisStream = millisStream.map(new TemperatureEventRecorder());

        HomogeneousIntervalGenerator<TemperatureEvent, TemperatureWarning> temp = new HomogeneousIntervalGenerator<>();
        Operation operationt1 = new OperandWrapper(Operand.Value);
        Operation operationt2 = new SingleValue(31);
        Expression expressiont = new ConditionNew(operationt1,Operator.GreaterThanEqual,operationt2);
        temp.source(millisStream)
                .sourceType(TemperatureEvent.class)
                .targetType(TemperatureWarning.class)
                .condition(new AbsoluteCondition(expressiont))
                .within(Time.milliseconds(10))
                .outputValue(Operand.Average)
                .produceOnlyMaximalIntervals(true);
        DataStream<TemperatureWarning> temperatureWarning = temp.runWithCEP();
        temperatureWarning.print();

        EventRecordGenerator<TemperatureWarning, EventRecord> tempRecord = new EventRecordGenerator<>();
        tempRecord.source(temperatureWarning)
                .sourceType(TemperatureWarning.class)
                .targetType(EventRecord.class)
                .expectedNum(1000000)
                .fPositive(0.01);
        DataStream<EventRecord> trydo = tempRecord.run();
        trydo.print();

        // secondsStream
        DataStream<PressureEvent> secondsStream = env.addSource(new PressureSource(1, 3, 20))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PressureEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp()));
        secondsStream = secondsStream.map(new PressureEventRecorder());
        HomogeneousIntervalGenerator<PressureEvent, PressureWarning> pre = new HomogeneousIntervalGenerator<>();
        Operation operationp1 = new OperandWrapper(Operand.Value);
        Operation operationp2 = new SingleValue(24);
        Expression expressionp = new ConditionNew(operationp1,Operator.GreaterThanEqual,operationp2);
        pre.source(secondsStream)
                .sourceType(PressureEvent.class)
                .targetType(PressureWarning.class)
                .condition(new AbsoluteCondition(expressionp))
                .within(Time.seconds(10))
                .outputValue(Operand.Average)
                .produceOnlyMaximalIntervals(true);
        DataStream<PressureWarning> pressureWarning = pre.runWithCEP();
        pressureWarning.print();

        IntervalOperator<TemperatureWarning, PressureWarning> matchOperator = new IntervalOperator<>();

        matchOperator.leftIntervalStream(temperatureWarning)
                .rightIntervalStream(pressureWarning)
                .within(Time.seconds(10))
                .filterForMatchType(Match.MatchType.Equals)
                .filterForMatchType(Match.MatchType.During)
                .filterForMatchType(Match.MatchType.Contains)
                .filterForMatchType(Match.MatchType.Overlaps)
                .filterForMatchType(Match.MatchType.Starts)
                .filterForMatchType(Match.MatchType.StartedBy)
                .filterForMatchType(Match.MatchType.Finishes)
                .filterForMatchType(Match.MatchType.FinishedBy)
                .filterForMatchType(Match.MatchType.Before)
                .filterForMatchType(Match.MatchType.After);

        DataStream<Match> matches = matchOperator.run();

        matches.print();
//        DataStream<TemperatureEvent> millisStream2 = env.addSource(new TemperatureSource(1, 1, 35))
//                .assignTimestampsAndWatermarks(WatermarkStrategy.<TemperatureEvent>forBoundedOutOfOrderness(Duration.ofMillis(10))
//                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp()));
//        millisStream2 = millisStream2.map(new TemperatureEventRecorder());
//        HomogeneousIntervalGenerator<TemperatureEvent, TemperatureWarning> temp2 = new HomogeneousIntervalGenerator<>();
//        Operation operationt3 = new OperandWrapper(Operand.Value);
//        Operation operationt4 = new SingleValue(33);
//        Expression expressiont2 = new ConditionNew(operationt3,Operator.GreaterThanEqual,operationt4);
//        temp2.source(millisStream2)
//                .sourceType(TemperatureEvent.class)
//                .targetType(TemperatureWarning.class)
//                .condition(new AbsoluteCondition(expressiont2))
//                .within(Time.milliseconds(10))
//                .outputValue(Operand.Average)
//                .produceOnlyMaximalIntervals(true);
//        DataStream<TemperatureWarning> temperatureWarning2 = temp.runWithCEP();
//        temperatureWarning2.print();
//        IntervalOperator.before(temperatureWarning, temperatureWarning2, Time.minutes(1)).print();
//        IntervalOperator.meets(temperatureWarning, temperatureWarning2, Time.minutes(1)).print();
//        IntervalOperator.equalTo(temperatureWarning, temperatureWarning2, Time.minutes(1)).print();
//        IntervalOperator.overlap(temperatureWarning, temperatureWarning2, Time.minutes(1)).print();
//        IntervalOperator.during(temperatureWarning, temperatureWarning2, Time.minutes(1)).print();
//        IntervalOperator.starts(temperatureWarning, temperatureWarning2, Time.minutes(1)).print();
//        IntervalOperator.finishes(temperatureWarning, temperatureWarning2, Time.minutes(1)).print();
//        IntervalOperator.contains(temperatureWarning, temperatureWarning2, Time.minutes(1)).print();
//        IntervalOperator.startsBy(temperatureWarning, temperatureWarning2, Time.minutes(1)).print();
//        IntervalOperator.overlapby(temperatureWarning, temperatureWarning2, Time.minutes(1)).print();
//        IntervalOperator.metBy(temperatureWarning, temperatureWarning2, Time.minutes(1)).print();
//        IntervalOperator.after(temperatureWarning, temperatureWarning2, Time.minutes(1)).print();

        //Step No 5
        //Trigger the programme execution by calling execute(), mode of execution (local or cluster).
        JobExecutionResult result = env.execute("CEP Interval job");
        //result.getAccumulatorResult("throughput");


    }
}
