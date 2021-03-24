package sparkJob.sparkCore.service.impl;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import sparkJob.SparkApp;
import sparkJob.mysql.DPMysql;
import sparkJob.sparkCore.service.SparkService;
import sparkJob.sparkStreaming.KafkaStreaming;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SparkMysqlDemo implements SparkService {
    @Override
    public <T> T execute(Map<String, Object> var) throws Exception {
        JavaRDD<Row> rowJavaRDD = DPMysql.rddRead("(select * from bigdata.user where 1=1) tmp");

        SparkSession session = SparkApp.getSession();
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("sex", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("age", DataTypes.StringType, true));
        StructType structType = DataTypes.createStructType(fields);
        Dataset<Row> dataFrame = session.createDataFrame(rowJavaRDD, structType);
        dataFrame.createOrReplaceTempView("chenjian");
        Dataset<Row> sql = session.sql("select * from chenjian");
        sql.show();
        StructField[] fields1 = sql.schema().fields();

        DPMysql.commonDatasetWriteBatch("jdbc:mysql://172.17.0.6:3306/bigdata?useSSL=false&allowMultiQueries=true&serverTimezone=Asia/Shanghai", "root", "root", "user", sql, SaveMode.Append);
//        DPMysql.commonDatasetWriteBatch("jdbc:mysql://127.0.0.1:3306/bigdata?useSSL=false&allowMultiQueries=true&serverTimezone=Asia/Shanghai","root","root","user", sql, SaveMode.Append);

        return null;
    }

    @Override
    public <T> T streaming(Map<String, Object> var, KafkaStreaming kafkaStreaming) throws Exception {
        return null;
    }
}
