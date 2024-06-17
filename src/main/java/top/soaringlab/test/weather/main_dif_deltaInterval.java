package top.soaringlab.test.weather;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import top.soaringlab.MTCICEP.generator.IntervalOperator;
import top.soaringlab.MTCICEP.generator.Match;
import top.soaringlab.test.weather.event.TempDifInterval;
import top.soaringlab.test.weather.event.TempTrendInterval;
import top.soaringlab.test.weather.event.TemperatureEvent;
import top.soaringlab.test.weather.oj.TemperatureLog;
import top.soaringlab.test.weather.processor.TempDifIntervalProcessor;
import top.soaringlab.test.weather.processor.TempTrendIntervalProcessor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collection;

import static top.soaringlab.test.weather.util.Common.convertStringToLong;
import static top.soaringlab.test.weather.util.Common.parseDoubleOrDefault;

/**
 * @author rillusory
 * @Description
 * @date 4/21/24 11:10 PM
 **/
public class main_dif_deltaInterval {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        // StreamTableEnvironment temTableEnv = StreamTableEnvironment.create(env, fsSettings);

        //创建集合，作为数据源
        Collection<TemperatureEvent> temperatureDataCollection = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new FileReader(System.getProperty("user.dir") + "/src/main/" +
                "java/top/soaringlab/test/weather/data/temp.csv"));//换成你的文件名
        reader.readLine();//第一行信息，为标题信息，不用,如果需要，注释掉
        String line = null;

        int index = 0;
        //从csv文件中挑选部分数据创建GeoMessage对象
        while ((line = reader.readLine()) != null) {
            index++;
            String[] items = line.split(","); // 按逗号分隔
            String timestamp = items[0]; // 时间戳
            double vancouver = parseDoubleOrDefault(items[1]); // Vancouver 温度
            double portland = parseDoubleOrDefault(items[2]); // Portland 温度
            double sanFrancisco = parseDoubleOrDefault(items[3]); // San Francisco 温度
            double seattle = parseDoubleOrDefault(items[4]); // Seattle 温度
            double losAngeles = parseDoubleOrDefault(items[5]); // Los Angeles 温度
            double sanDiego = parseDoubleOrDefault(items[6]); // San Diego 温度
            double lasVegas = parseDoubleOrDefault(items[7]); // Las Vegas 温度
            double phoenix = parseDoubleOrDefault(items[8]); // Phoenix 温度
            double albuquerque = parseDoubleOrDefault(items[9]); // Albuquerque 温度
            double denver = parseDoubleOrDefault(items[10]); // Denver 温度
            double sanAntonio = parseDoubleOrDefault(items[11]); // San Antonio 温度
            double dallas = parseDoubleOrDefault(items[12]); // Dallas 温度
            double houston = parseDoubleOrDefault(items[13]); // Houston 温度
            double kansasCity = parseDoubleOrDefault(items[14]); // Kansas City 温度
            double minneapolis = parseDoubleOrDefault(items[15]); // Minneapolis 温度
            double saintLouis = parseDoubleOrDefault(items[16]); // Saint Louis 温度
            double chicago = parseDoubleOrDefault(items[17]); // Chicago 温度
            double nashville = parseDoubleOrDefault(items[18]); // Nashville 温度
            double indianapolis = parseDoubleOrDefault(items[19]); // Indianapolis 温度
            double atlanta = parseDoubleOrDefault(items[20]); // Atlanta 温度
            double detroit = parseDoubleOrDefault(items[21]); // Detroit 温度
            double jacksonville = parseDoubleOrDefault(items[22]); // Jacksonville 温度
            double charlotte = parseDoubleOrDefault(items[23]); // Charlotte 温度
            double miami = parseDoubleOrDefault(items[24]); // Miami 温度
            double pittsburgh = parseDoubleOrDefault(items[25]); // Pittsburgh 温度
            double toronto = parseDoubleOrDefault(items[26]); // Toronto 温度
            double philadelphia = parseDoubleOrDefault(items[27]); // Philadelphia 温度
            double newYork = parseDoubleOrDefault(items[28]); // New York 温度
            double montreal = parseDoubleOrDefault(items[29]); // Montreal 温度
            double boston = parseDoubleOrDefault(items[30]); // Boston 温度
            double beersheba = parseDoubleOrDefault(items[31]); // Beersheba 温度
            double telAvivDistrict = parseDoubleOrDefault(items[32]); // Tel Aviv District 温度
            double eilat = parseDoubleOrDefault(items[33]); // Eilat 温度
            double haifa = parseDoubleOrDefault(items[34]); // Haifa 温度
            double nahariyya = parseDoubleOrDefault(items[35]); // Nahariyya 温度
            double jerusalem = parseDoubleOrDefault(items[36]); // Jerusalem 温度

            // 创建 TemperatureLog 对象并添加到集合中
            TemperatureLog temperatureData = new TemperatureLog(timestamp, vancouver, portland, sanFrancisco, seattle, losAngeles, sanDiego, lasVegas, phoenix, albuquerque,
                    denver, sanAntonio, dallas, houston, kansasCity, minneapolis, saintLouis, chicago, nashville, indianapolis,
                    atlanta, detroit, jacksonville, charlotte, miami, pittsburgh, toronto, philadelphia, newYork, montreal,
                    boston, beersheba, telAvivDistrict, eilat, haifa, nahariyya, jerusalem);

            String[] cities = temperatureData.getCities();
            for (int i = 0; i < cities.length; i++) {
                String city = cities[i];
                TemperatureEvent temperatureEvent = new TemperatureEvent(convertStringToLong(timestamp), city, temperatureData.getTemperatureByName(city));
                temperatureDataCollection.add(temperatureEvent);
            }
        }
        System.out.println(index);
        // 给流环境设置数据源
        DataStream<TemperatureEvent> temperatureEvents = env.fromCollection(temperatureDataCollection);

        long beginTime = System.currentTimeMillis();
        SingleOutputStreamOperator<TempDifInterval> tempDifIntervalEvent = temperatureEvents.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<TemperatureEvent>(Time.minutes(1)) {
                    @Override
                    public long extractTimestamp(TemperatureEvent temperatureEvent) {
                        return temperatureEvent.f1.getValue();
                    }
                })
                .keyBy(temp -> temp.getKey())
                .window(SlidingEventTimeWindows.of(Time.days(5), Time.days(1)))
                .process(new TempDifIntervalProcessor());
//                .print();


        // 识别趋势事件
        DataStreamSource<TemperatureEvent> temForTrend = env.fromCollection(temperatureDataCollection);

        SingleOutputStreamOperator<TempTrendInterval> temTrendIntervalEvent = temForTrend.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<TemperatureEvent>(Time.minutes(1)) {
                    @Override
                    public long extractTimestamp(TemperatureEvent temperatureEvent) {
                        return temperatureEvent.f1.getValue();
                    }
                })
                .keyBy(temp -> temp.getKey())
                .window(SlidingEventTimeWindows.of(Time.days(30), Time.days(5)))
                .process(new TempTrendIntervalProcessor());


        IntervalOperator<TempDifInterval, TempTrendInterval> matchOperator = new IntervalOperator<>();

        matchOperator.leftIntervalStream(tempDifIntervalEvent)
                .rightIntervalStream(temTrendIntervalEvent)
                .within(Time.days(30))
                .filterForMatchType(Match.MatchType.During);

        DataStream<Match> matches = matchOperator.run();

        // 因为 flink sql 不支持 subset ， 所以该实验无法进行
        //对数据通过map算子进行处理，具体方法为自定义方法
        //DataStream<TemperatureEvent> temperatureEvents = input.flatMap(new ParseTemperatureEvent());

//        Table temTable = temTableEnv.fromDataStream(temperatureEvents, $("f0"), $("f1"), $("f2"), $("f3"));
//        temTableEnv.createTemporaryView("Weather", temTable);
        //Table coldWaveInterval = temTableEnv.sqlQuery("select f0 from Weather");
        //temTableEnv.toDataStream(coldWaveInterval).print("convert");



//        Table coldWaveInterval = temTableEnv.sqlQuery("SELECT * \n" +
//                "FROM Weather\n" +
//                "MATCH_RECOGNIZE(\n" +
//                "    PARTITION BY f0 \n" +
//                "    ORDER BY f1 \n" +
//                "\n" +
//                "    PATTERN ((W1 (DOWN & FALL & W2) W1) & UP_MK & WINDOW) \n" +
//                "\n" +
//                "    DEFINE SEGMENT W1 AS true,\n" +
//                "       SEGMENT W2 AS window(1, 5),\n" +
//                "        D AS f1 - first(D.f1) <= 5 * 24 * 60 * 60 * 1000, \n" +
//                "        Z AS last(U.f1) - first(U.f1) BETWEEN 25 * 24 * 60 * 60 * 1000 AND 30 * 24 * 60 * 60 * 1000\n" +
//                "            AND mann_kendall_test(U.f2) >= 3.0 \n" +
//                "            AND linear_regression_r2(D.f1, D.f2) >= 0.95 \n" +
//                "            AND last(D.f2) - first(D.f2) < -20 \n" +
//                ")\n");

        //System.out.println(coldWaveInterval.explain());

        //启动任务执行，execute可以不给参数，参数是作业名字
        env.execute("Java WordCount from SocketTextStream Example");
        long endTime = System.currentTimeMillis();
        System.out.println(endTime - beginTime);
    }



}
