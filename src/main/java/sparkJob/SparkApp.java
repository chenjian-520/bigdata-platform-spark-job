package sparkJob;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.CollectionAccumulator;
import scala.Serializable;
import sparkJob.common.PermissionManager;
import sparkJob.sparkCore.domain.RuleJson;
import sparkJob.sparkCore.service.sparkService;
import sparkJob.sparkStreaming.KafkaStreaming;
import sparkJob.sparkStreaming.domain.DPKafkaInfo;

public class SparkApp implements Serializable {

    public static CollectionAccumulator<SparkSession> sessionBroadcast;

    public static CollectionAccumulator<JavaSparkContext> contextBroadCast;

    public static PermissionManager permissionManager;

    public static Broadcast<PermissionManager> permissionBroadcast;

    public static Broadcast<DPKafkaInfo> kafkaInfoBroadcast;

    public static Broadcast<RuleJson> ruleJsonBroadcast;

    public static void main(String[] args) throws Exception {

        //获取arg参数
        String arg = args[0];
        System.out.println(arg);
        JSONObject appParam = JSON.parseObject(arg);
        String dpType = appParam.getString("dpType");
        SparkSession sparkSession = null;
        SparkContext sparkContext = null;

        try {
            sparkSession = SparkSession.builder()
                    .appName(appParam.getString("appName"))
                    .master("local[*]")
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .getOrCreate();

            sparkContext = sparkSession.sparkContext();
            JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);

            //必要广播变量
            contextBroadCast = sparkContext.collectionAccumulator("javacontext");
            contextBroadCast.add(javaSparkContext);
            sessionBroadcast = sparkContext.collectionAccumulator("sparksession");
            sessionBroadcast.add(sparkSession);
            String streamingParams = appParam.getString("kafkaStreaming");
            DPKafkaInfo dpKafkaInfo = JSON.parseObject(streamingParams, DPKafkaInfo.class);
            kafkaInfoBroadcast = javaSparkContext.broadcast(dpKafkaInfo);
            permissionManager = (PermissionManager) Class.forName("sparkJob.common.ProdPermissionManager").newInstance();
            permissionBroadcast = javaSparkContext.broadcast(permissionManager);

            //规则参数
            String rule = appParam.getString("rule");
            RuleJson ruleJson = JSON.parseObject(rule, RuleJson.class);
            ruleJsonBroadcast = javaSparkContext.broadcast(ruleJson);

            if ("streaming".equals(dpType)) {
                KafkaStreaming kafkaStreaming = new KafkaStreaming();
                kafkaStreaming.init();
                Class<?> serviceclazz = Class.forName(appParam.getString("sericeName"));
                sparkService sparkservice = (sparkService) serviceclazz.newInstance();
                sparkservice.streaming(appParam, kafkaStreaming);
            } else {
                Class<?> serviceclazz = Class.forName(appParam.getString("sericeName"));
                sparkService sparkservice = (sparkService) serviceclazz.newInstance();
                sparkservice.execute(appParam);
            }

        } finally {
            if (sparkContext != null) {
                sparkContext.stop();
            }
            if (sparkSession != null) {
                sparkSession.stop();
            }
        }
    }

    public static SparkSession getSession() {
        SparkSession sparkSession = sessionBroadcast.value().get(0);
        return sparkSession;
    }

    public static JavaSparkContext getContext() {
        JavaSparkContext javaSparkContext = contextBroadCast.value().get(0);
        return javaSparkContext;
    }

    public static DPKafkaInfo getDPKafkaInfo() {
        return (DPKafkaInfo) kafkaInfoBroadcast.value();
    }

    public static PermissionManager getDpPermissionManager() {
        return (PermissionManager) permissionBroadcast.value();
    }

}
