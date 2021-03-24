package sparkJob.sparkCore.service.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.lib.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import sparkJob.SparkApp;
import sparkJob.hdfs.SystemFile;
import sparkJob.sparkCore.domain.RuleJson;
import sparkJob.sparkCore.service.SparkService;
import sparkJob.sparkStreaming.KafkaStreaming;

import java.util.Map;
import java.util.ResourceBundle;

/**
 * sparkCode 实现逻辑demo
 */
public class SparkDemo implements SparkService {

    @Override
    public <T> T execute(Map<String, Object> var) throws Exception {

        RuleJson value = SparkApp.ruleJsonBroadcast.value();
        ResourceBundle resourceBundle = ResourceBundle.getBundle("ProPermissionManager");
        String mysqlUrl = resourceBundle.getString("mysqlUrl");
        System.out.println(mysqlUrl);
        //读写本地文件
        JavaRDD<String> stringJavaRDD = SystemFile.readSystemFile(value.getFileInPath(), 1);
        stringJavaRDD.take(10).forEach(r -> System.out.println(r));
        System.out.println(value.getFileOutPath());

//        SystemFile.saveSystemFile(stringJavaRDD,value.getFileOutPath());

        JavaPairRDD<String, String> stringStringJavaPairRDD = stringJavaRDD.keyBy(r -> r);

        // spark 写 hdfs
        Configuration hadoopConf = stringStringJavaPairRDD.context().hadoopConfiguration();
        hadoopConf.set("mapreduce.output.fileoutputformat.compress", "true");
        hadoopConf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");
        hadoopConf.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
        stringStringJavaPairRDD.saveAsNewAPIHadoopFile("hdfs://172.17.0.3:9000/user",
                CombineTextInputFormat.class,
                CombineTextInputFormat.class,
                TextOutputFormat.class);
        System.out.println("----------------------------------");
        stringJavaRDD.saveAsTextFile(value.getFileOutPath());

        return null;
    }

    @Override
    public <T> T streaming(Map<String, Object> var1, KafkaStreaming kafkaStreaming) throws Exception {
        return null;
    }
}
