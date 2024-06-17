package top.soaringlab.test.weather;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import top.soaringlab.MTCICEP.generator.IntervalOperator;
import top.soaringlab.MTCICEP.generator.Match;
import top.soaringlab.test.weather.event.TempDifInterval;
import top.soaringlab.test.weather.event.TempTrendInterval;
import top.soaringlab.test.weather.event.TemperatureEvent;
import top.soaringlab.test.weather.math.TempDiff;
import top.soaringlab.test.weather.math.MannKendall;
import top.soaringlab.test.weather.oj.TemperatureLog;
import top.soaringlab.test.weather.processor.ParseTemperatureEvent;
import top.soaringlab.test.weather.processor.TempDifIntervalProcessor;
import top.soaringlab.test.weather.processor.TempTrendIntervalProcessor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static top.soaringlab.test.weather.util.Common.convertStringToLong;
import static top.soaringlab.test.weather.util.Common.parseDoubleOrDefault;

/**
 * @author rillusory
 * @Description
 * @date 4/21/24 11:10 PM
 **/
public class main_sql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment temTableEnv = StreamTableEnvironment.create(env, fsSettings);

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

        // 给流环境设置数据源
        DataStream<TemperatureEvent> temperatureEvents = env.fromCollection(temperatureDataCollection);

        Table temTable = temTableEnv.fromDataStream(temperatureEvents,  Schema.newBuilder()
                .column("f0", "STRING")
                .column("f1", "BIGINT") // 原始时间戳列
                .column("f2", "DOUBLE")
                .column("f3", "STRING")
                .columnByExpression("f1_ts", "TO_TIMESTAMP_LTZ(f1, 3)") // 将 BIGINT 转换为 TIMESTAMP_LTZ
                .watermark("f1_ts", "f1_ts - INTERVAL '5' SECOND") // 设置 Watermark
                .build());
        temTableEnv.createTemporaryView("Weather", temTable);
//        Table coldWaveInterval = temTableEnv.sqlQuery("select f0 from Weather");
//        temTableEnv.toDataStream(coldWaveInterval).print("convert");

        // 创建自定义函数
        temTableEnv.createTemporarySystemFunction("MannKendallTest", new MannKendall());
        temTableEnv.createTemporarySystemFunction("CheckTempDiffWithin5Days", new TempDiff());

//        // flink sql 不存在 subset 语句，所以需要对原有数据进行改写
//        Table coldWaveInterval = temTableEnv.sqlQuery("SELECT * \n" +
//                "FROM Weather \n" +
//                "MATCH_RECOGNIZE (\n" +
//                "    PARTITION BY f0 \n" +
//                "    ORDER BY f1_ts\n" +
//                "    MEASURES\n" +
//                "        LAST(D.f1) AS end_time,\n" +
//                "        FIRST(D.f1) AS start_time,\n" +
//                "        COUNT(D.f1) AS num_events,\n" +
//                "        mann_kendall_test(f2) AS mk_test,\n" +
////                "        linear_regression_r2(D.f1, D.f2) AS lr_r2,\n" +
//                "        LAST(D.f2) - FIRST(D.f2) AS temp_diff\n" +
//                "    PATTERN (A* D+ B* Z)\n" +
//                "    DEFINE \n" +
//                "        A AS true,\n" +
//                "        D AS CAST(f1 AS BIGINT) - CAST(FIRST(D.f1) AS BIGINT) <= 5 * 24 * 60 * 60 * 1000,\n" +
//                "        B AS true,\n" +
//                "        Z AS CAST(LAST(D.f1) AS BIGINT) - CAST(FIRST(D.f1) AS BIGINT) BETWEEN 25 * 24 * 60 * 60 * 1000 AND 30 * 24 * 60 * 60 * 1000\n" +
//                "            AND mann_kendall_test(f2) >= 3.0\n" +
////                "            AND linear_regression_r2(D.f1, D.f2) >= 0.95\n" +
//                "            AND CAST(LAST(D.f2) AS BIGINT) - CAST(FIRST(D.f2) AS BIGINT) < -20\n" +
//                ") AS MR");
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


//        Table coldWaveInterval = temTableEnv.sqlQuery("WITH WeatherWithDiffs AS (\n" +
//                "    SELECT\n" +
//                "        f0,\n" +
//                "        f1_ts AS start_time,\n" +
//                "        LEAD(f1_ts, 1) OVER (PARTITION BY f0 ORDER BY f1_ts) AS end_time,\n" +
//                "        f2 AS start_temp,\n" +
//                "        LEAD(f2, 1) OVER (PARTITION BY f0 ORDER BY f1_ts) AS end_temp,\n" +
//                "        TIMESTAMPDIFF(DAY, f1_ts, LEAD(f1_ts, 1) OVER (PARTITION BY f0 ORDER BY f1_ts)) AS time_diff,\n" +
//                "        LEAD(f2, 1) OVER (PARTITION BY f0 ORDER BY f1_ts) - f2 AS temp_diff,\n" +
//                "        COLLECT(f2) OVER (PARTITION BY f0 ORDER BY f1_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS temp_list\n" +
//                "    FROM Weather\n" +
//                ")\n" +
//                "SELECT *\n" +
//                "FROM WeatherWithDiffs\n" +
//                "LATERAL TABLE(mann_kendall_test(temp_list)) AS T(mk_result) \n" +
//                "WHERE mk_result >= 3.0\n" +
//                "  AND time_diff BETWEEN 25 AND 30\n" +
//                "  AND EXISTS (\n" +
//                "      SELECT 1\n" +
//                "      FROM (\n" +
//                "          SELECT\n" +
//                "              f0,\n" +
//                "              f1_ts AS inner_start_time,\n" +
//                "              LEAD(f1_ts, 1) OVER (PARTITION BY f0 ORDER BY f1_ts) AS inner_end_time,\n" +
//                "              LEAD(f2, 1) OVER (PARTITION BY f0 ORDER BY f1_ts) - f2 AS temp_diff,\n" +
//                "              TIMESTAMPDIFF(DAY, f1_ts, LEAD(f1_ts, 1) OVER (PARTITION BY f0 ORDER BY f1_ts)) AS inner_time_diff\n" +
//                "          FROM Weather\n" +
//                "      ) AS InnerWeather\n" +
//                "      WHERE InnerWeather.f0 = WeatherWithDiffs.f0\n" +
//                "        AND InnerWeather.inner_start_time >= WeatherWithDiffs.start_time\n" +
//                "        AND InnerWeather.inner_end_time <= WeatherWithDiffs.end_time\n" +
//                "        AND inner_time_diff <= 5\n" +
//                "        AND temp_diff <= -20\n" +
//                "  )\n");



        // 创建视图，聚合每个窗口的温度列表和时间戳列表
        temTableEnv.executeSql(
                "CREATE VIEW WeatherWithTempList AS " +
                        "SELECT " +
                        "f0, " +
                        "TUMBLE_START(f1_ts, INTERVAL '25' DAY) AS window_start, " +
                        "TUMBLE_END(f1_ts, INTERVAL '25' DAY) AS window_end, " +
                        "COLLECT(f2) AS temp_list, " +
                        "CAST(COLLECT(f1_ts) AS ARRAY<TIMESTAMP>) tstamp_list " +
                        "FROM Weather " +
                        "GROUP BY f0, TUMBLE(f1_ts, INTERVAL '25' DAY)"
        );

        // 使用Table API进行复杂查询
        Table weatherWithTempList = temTableEnv.from("WeatherWithTempList");

        // 查询并使用自定义函数
        Table result = weatherWithTempList
                .filter(call("MannKendallTest", $("temp_list")).isGreaterOrEqual(3.0))
                .filter(call("CheckTempDiffWithin5Days", $("temp_list"), $("tstamp_list")).isEqual(true))
                .select($("f0"), $("window_start"), $("window_end"), $("temp_list"));

        // 打印结果
        temTableEnv.toDataStream(result, Row.class).print();

        //启动任务执行，execute可以不给参数，参数是作业名字
        env.execute("Java WordCount from SocketTextStream Example");

    }



}
