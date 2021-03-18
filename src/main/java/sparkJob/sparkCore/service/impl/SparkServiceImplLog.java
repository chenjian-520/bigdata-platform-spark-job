package sparkJob.sparkCore.service.impl;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrameReader;
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
import sparkJob.sparkCore.domain.mysqlBean.NginxLog;
import sparkJob.sparkCore.service.sparkService;
import sparkJob.sparkStreaming.KafkaStreaming;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;

import static org.codehaus.commons.compiler.samples.DemoBase.explode;

public class SparkServiceImplLog implements sparkService {
    @Override
    public <T> T execute(Map<String, Object> var) throws Exception {

        RuleJson value = SparkApp.ruleJsonBroadcast.value();
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
        SimpleDateFormat sdf2=new SimpleDateFormat("yyyy-MM-dd");

        // 解析 json格式 写入mysql 样例
        SparkSession session = SparkApp.getSession();
        JavaRDD<NginxLog> map = session.read().json(value.getFileInPath()).javaRDD().map(line -> {
            NginxLog nginxlog = new NginxLog();
            nginxlog.setRemoteAddr(line.getAs("remote_addr"));
            nginxlog.setHttpXForwardedFor(line.getAs("http_x_forwarded_for"));
            Date parse = sdf.parse(line.getAs("time_local"));
            nginxlog.setTimeLocal(sdf2.format(parse));
            nginxlog.setStatus(line.getAs("status"));
            nginxlog.setBodyBytesSent(line.getAs("body_bytes_sent"));
            nginxlog.setHttpUserAgent(line.getAs("http_user_agent"));
            nginxlog.setHttpReferer(line.getAs("http_referer"));
            nginxlog.setRequestMethod(line.getAs("request_method"));
            nginxlog.setRequestTime(line.getAs("request_time"));
            nginxlog.setRequestUri(line.getAs("request_uri"));
            nginxlog.setServerProtocol(line.getAs("server_protocol"));
            nginxlog.setRequestBody(line.getAs("request_body"));
            nginxlog.setHttpToken(line.getAs("http_token"));
            return nginxlog;
        });
        Dataset<Row> dataFrame = session.createDataFrame(map, NginxLog.class);
        dataFrame.write().option("sep",",").csv("C:\\Users\\issuser\\Desktop\\log\\log.csv");

//        DPMysql.commonOdbcWriteBatch("nginx_log",dataFrame);
//
//        //读写本地文件
//        JavaRDD<String> stringJavaRDD = SystemFile.readSystemFile(value.getFileInPath(), 1);
//
//        stringJavaRDD.map(r->{
//            String[] split = r.split(value.getSeparator());
//            int a = 3;
//            String s = split[a];
//            return r.toString()+","+s;
//        });
//
//        SystemFile.saveSystemFile(stringJavaRDD,value.getFileOutPath());
//
//        // 解析xml https://github.com/databricks/spark-xml 写入mysql
//
//        StructType customSchema = new StructType(new StructField[] {
//                new StructField("_id", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("author", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("description", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("genre", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("price", DataTypes.DoubleType, true, Metadata.empty()),
//                new StructField("publish_date", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("title", DataTypes.StringType, true, Metadata.empty())
//        });
//
//        Dataset<Row> load = session.read()
//                .format("xml")
//                .option("rootTag", "catalog")
//                .option("rowTag", "book")
//                .schema(customSchema)
//                .load("C:\\Users\\issuser\\Desktop\\log\\log.xml");
//        load.show();
//        DPMysql.commonOdbcWriteBatch("log_xml",load);


        return null;
    }

    @Override
    public <T> T streaming(Map<String, Object> var, KafkaStreaming kafkaStreaming) throws Exception {
        return null;
    }
}
