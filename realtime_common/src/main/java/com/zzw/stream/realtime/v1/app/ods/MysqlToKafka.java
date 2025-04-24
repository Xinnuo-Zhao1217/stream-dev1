package com.zzw.stream.realtime.v1.app.ods;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.zzw.stream.realtime.v1.utils.FlinkSinkUtil;
import com.zzw.stream.realtime.v1.utils.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.zzw.stream.realtime.v1.app.ods.MysqlToKafka
 * @Author zhengwei_zhou
 * @Date 2025/4/17 9:00
 * @description: MysqlToKafka
 */

public class MysqlToKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        MySqlSource<String> realtimeV1 = FlinkSourceUtil.getMySqlSource("realtime_v1", "*");

        DataStreamSource<String> mySQLSource = env.fromSource(realtimeV1, WatermarkStrategy.noWatermarks(), "MySQL Source");

//        mySQLSource.print();

        KafkaSink<String> topic_db = FlinkSinkUtil.getKafkaSink("xinnuo_zhao_db");

        mySQLSource.sinkTo(topic_db);

        env.execute("Print MySQL Snapshot + Binlog");

    }
}



