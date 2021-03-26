package sparkJob.sparkCore.service.impl;

import lombok.val;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import sparkJob.SparkApp;
import sparkJob.sparkCore.service.SparkService;
import sparkJob.sparkStreaming.KafkaStreaming;

import java.util.Collection;
import java.util.Map;

/**
 * http://spark.apache.org/docs/3.0.1/structured-streaming-kafka-integration.html
 * kafka sparkStreaming结构化处理
 */
public class SparkStreamingKafka implements SparkService {
    @Override
    public <T> T execute(Map<String, Object> var) throws Exception {
        return null;
    }

    @Override
    public <T> T streaming(Map<String, Object> var, KafkaStreaming kafkaStreaming) throws Exception {
        Map kafkaParams = kafkaStreaming.getKafkaParams();
        Collection topics = kafkaStreaming.getTopics();

        // kafka spark3.X 批处理新特性
        OffsetRange[] offsetRanges = {
                // topic, partition, inclusive starting offset, exclusive ending offset
                OffsetRange.create(topics.toArray()[0].toString(), 0, 0, 100),
                OffsetRange.create(topics.toArray()[0].toString(), 1, 0, 100)
        };
        kafkaParams.put("auto.offset.reset", "latest");
        StructType customSchema = new StructType(new StructField[]{
                new StructField("string", DataTypes.StringType, true, Metadata.empty())
        });
        JavaRDD<Row> rdd = KafkaUtils.createRDD(
                SparkApp.getContext(),
                kafkaParams,
                offsetRanges,
                LocationStrategies.PreferConsistent()
        ).map(new Function<ConsumerRecord<String, Object>, GenericRowWithSchema>() {
            @Override
            public GenericRowWithSchema call(ConsumerRecord<String, Object> v1) throws Exception {

                return new GenericRowWithSchema(new String[]{String.valueOf(v1.value())}, customSchema);
            }
        });
        Dataset<Row> dataFrame = SparkApp.getSession().createDataFrame(rdd, customSchema);
        dataFrame.show();

        //spark 结构化流处理
//        Dataset<Row> df1 = SparkApp.getSession()
//                .readStream()
//                .format("kafka")
//                .option("kafka.bootstrap.servers", kafkaParams.get("bootstrap.servers").toString())
//                .option("subscribePattern", "topic.*")
//                .load();
//        StreamingQuery console = df1.writeStream().format("console").start();
//        df1.printSchema();
//        console.awaitTermination();

        Dataset<Row> df = SparkApp.getSession()
                .read()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaParams.get("bootstrap.servers").toString())
                .option("subscribePattern", "topic.*")
                .option("startingOffsets", "earliest")
                .option("endingOffsets", "latest")
                .load();
        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").show();


        return null;
    }
}
