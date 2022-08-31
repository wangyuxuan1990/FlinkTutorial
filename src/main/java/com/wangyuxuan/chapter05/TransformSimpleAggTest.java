package com.wangyuxuan.chapter05;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wangyuxuan
 * @date 2022/6/27 7:29 下午
 * @Description
 */
public class TransformSimpleAggTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Bob", "./home", 3500L),
                new Event("Alice", "./prod?id=200", 3200L),
                new Event("Bob", "./prod?id=2", 3800L),
                new Event("Bob", "./prod?id=3", 4200L)
        );

        // 按键分组之后进行聚合，提取当前用户最近一次访问数据
        stream.keyBy(new KeySelector<Event, String>() {
                    @Override
                    public String getKey(Event value) throws Exception {
                        return value.user;
                    }
                }).max("timestamp")
                .print("max: ");

        stream.keyBy(data -> data.user)
                .maxBy("timestamp")
                .print("maxBy: ");

        env.execute();
    }
}
