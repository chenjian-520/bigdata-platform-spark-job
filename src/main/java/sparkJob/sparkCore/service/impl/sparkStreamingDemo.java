package sparkJob.sparkCore.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.CollectionAccumulator;
import sparkJob.SparkApp;
import sparkJob.sparkCore.service.sparkService;
import sparkJob.sparkStreaming.KafkaSink;
import sparkJob.sparkStreaming.KafkaStreaming;

import java.util.*;

public class sparkStreamingDemo implements sparkService {
    @Override
    public <T> T execute(Map<String, Object> var) throws Exception {
        return null;
    }

    @Override
    public <T> T streaming(Map<String, Object> var, KafkaStreaming kafkaStreaming) throws Exception {
        //总体逻辑，每次从流中获取ods中的数据，将缓存数据存到hbase并发送到kafka中，缓存清空
        final Broadcast kafkaProducer = SparkApp.getContext().broadcast(KafkaSink.apply(new Properties() {{
            putAll(kafkaStreaming.getKafkaParams());
        }}));

        //声明累加器缓存
        CollectionAccumulator<String> kafkaWriteList = SparkApp.getContext().sc().collectionAccumulator();

        //接收流式数据
        kafkaStreaming.startJob(rdd -> {
            rdd.foreachPartition(i -> {
                i.forEachRemaining(record -> {
                    String data = record.value().toString();
                    kafkaWriteList.add(data);
                });
            });
            //发送到kafka
            kafkaWriteList.value().forEach(dwdoutput -> {
                ((KafkaSink) kafkaProducer.value()).sendAll("topic3", dwdoutput);
            });
        });

        return null;
    }
}
