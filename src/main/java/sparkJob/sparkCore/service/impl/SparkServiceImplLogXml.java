package sparkJob.sparkCore.service.impl;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import sparkJob.SparkApp;
import sparkJob.hdfs.SystemFile;
import sparkJob.mysql.DPMysql;
import sparkJob.sparkCore.domain.RuleJson;
import sparkJob.sparkCore.service.SparkService;
import sparkJob.sparkStreaming.KafkaStreaming;

import java.util.Map;

public class SparkServiceImplLogXml implements SparkService {
    @Override
    public <T> T execute(Map<String, Object> var) throws Exception {
        RuleJson value = SparkApp.ruleJsonBroadcast.value();
        SparkSession session = SparkApp.getSession();
        //读写本地文件
        JavaRDD<String> stringJavaRDD = SystemFile.readSystemFile(value.getFileInPath(), 1);
        stringJavaRDD.map(r -> {
            String[] split = r.split(value.getSeparator());
            int a = 3;
            String s = split[a];
            return r.toString() + "," + s;
        });

        SystemFile.saveSystemFile(stringJavaRDD, value.getFileOutPath());

        // 解析xml https://github.com/databricks/spark-xml 写入mysql

        StructType customSchema = new StructType(new StructField[]{
                new StructField("_id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("author", DataTypes.StringType, true, Metadata.empty()),
                new StructField("description", DataTypes.StringType, true, Metadata.empty()),
                new StructField("genre", DataTypes.StringType, true, Metadata.empty()),
                new StructField("price", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("publish_date", DataTypes.StringType, true, Metadata.empty()),
                new StructField("title", DataTypes.StringType, true, Metadata.empty())
        });

        Dataset<Row> load = session.read()
                .format("xml")
                .option("rootTag", "catalog")
                .option("rowTag", "book")
                .schema(customSchema)
                .load("C:\\Users\\issuser\\Desktop\\log\\log.xml");
        load.show();
        DPMysql.commonOdbcWriteBatch("log_xml", load);
        return null;
    }

    @Override
    public <T> T streaming(Map<String, Object> var, KafkaStreaming kafkaStreaming) throws Exception {
        return null;
    }
}
