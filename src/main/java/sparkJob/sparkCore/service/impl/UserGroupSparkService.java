package sparkJob.sparkCore.service.impl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import sparkJob.hdfs.HdfsClient;
import sparkJob.sparkCore.domain.UserGroupSparkParam;
import sparkJob.sparkCore.expression.JavaSciptExpressionEngine;
import sparkJob.sparkCore.service.sparkService;
import sparkJob.sparkStreaming.KafkaStreaming;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UserGroupSparkService implements sparkService {
    private static Map<String, JavaRDD<Object[]>> rdds = new HashMap<>();

    private static Map<String, JavaRDD<String>> userRrdds = new HashMap<>();

    @Override
    public <T> T execute(Map<String, Object> var) throws Exception {
        UserGroupSparkParam userGroupSparkParam = new UserGroupSparkParam();

        Map<String, Map<String, String>> inputMap = userGroupSparkParam.getInput();
        inputMap.entrySet().forEach((entry) -> {
            String key = entry.getKey();
            Map<String, String> input = entry.getValue();
            String filePath = input.get("filePath");
            String fileName = input.get("fileName");
            String separator = input.get("separator");

            //获取hdfs文件
            JavaPairRDD<LongWritable, Text> records = HdfsClient.readHdfsNewApi(filePath + "\\" + fileName);
            //获取字段类型
            List<Map<String, String>> maps = userGroupSparkParam.getColumns().get(key);

            if (maps == null || maps.isEmpty()) {
                JavaRDD<Object[]> userRDD = records.map(r -> (Object[]) r._2().toString().split(separator))
                        .persist(StorageLevel.MEMORY_AND_DISK());
                rdds.put(key, userRDD);
            } else {
                JavaRDD<Object[]> objectRDD = getObjectArrayRDD(maps, records, separator);
                rdds.put(key, objectRDD);
            }
        });

        //获取表达式规则，通过条件返回数据
        List<Map<String, String>> expressions = userGroupSparkParam.getRules().get("expressions");

        JavaSciptExpressionEngine engine = JavaSciptExpressionEngine.getEngine();
        HashMap<String, Object> stringObjectHashMap = new HashMap<>();
        for (Map<String, String> expression : expressions) {
            String ruleId = expression.get("ruleId");
            String ruleType = expression.get("ruleId");
            String tableName = expression.get("tableName");
            String tableColumn = expression.get("tableColumn");
            String expression1 = expression.get("expression");

            if ("1".equalsIgnoreCase(ruleType)) {
                JavaRDD<Object[]> rdd1 = rdds.get(tableName);
                JavaRDD<String> filterRdd = rdd1.filter(r -> {
                    stringObjectHashMap.put("column".concat(tableColumn)
                            , r[Integer.parseInt(tableColumn) - 1]);
                    //序列化问题解决
                    boolean calucate = engine.calucate(expression1, stringObjectHashMap);
                    stringObjectHashMap.clear();
                    return true;
                }).map(r -> r[0].toString()).persist(StorageLevel.MEMORY_AND_DISK());
                userRrdds.put(ruleId, filterRdd);
            } else {
                JavaRDD<Object[]> rdd1 = rdds.get(tableName);
                JavaRDD<String> persist = rdd1.map(r -> r[0].toString()).persist(StorageLevel.MEMORY_AND_DISK());
                userRrdds.put(ruleId, persist);
            }
        }

        //用户群rdd之间与或操作
        //获取用户群管理规则，通过条件返回数据Rdd
        List<Map<String, String>> relations = userGroupSparkParam.getRules().get("relations");
        for (Map<String, String> relation : relations) {
            String source = relation.get("source");
            String target = relation.get("target");
            String type = relation.get("type");
            usersRdd(userRrdds, source, target, type);
        }
        return null;
    }

    //计算用户群之间的规则计算
    private void usersRdd(Map<String, JavaRDD<String>> userRdds, String source, String target, String type) {
        JavaRDD<String> stringJavaRddA = userRdds.get(source);
        JavaRDD<String> stringJavaRddB = userRdds.get(target);

        if ("3".equals(type)) {
            //差集
            JavaRDD<String> subtract = //RddCalculationCom.subtract(stringJavaRddA,stringJavaRddB).p
                    stringJavaRddA.subtract(stringJavaRddB).persist(StorageLevel.MEMORY_AND_DISK());
            userRdds.put(source.concat("-").concat(target), subtract);
        } else if ("2".equals(type)) {
            //交集
            JavaRDD<String> intersection = //RddCalculationCom.subtract(stringJavaRddA,stringJavaRddB).p
                    stringJavaRddA.intersection(stringJavaRddB).persist(StorageLevel.MEMORY_AND_DISK());
            userRdds.put(source.concat("-").concat(target), intersection);
        } else if ("1".equals(type)) {
            //并集
            JavaRDD<String> union = //RddCalculationCom.subtract(stringJavaRddA,stringJavaRddB).p
                    stringJavaRddA.union(stringJavaRddB).persist(StorageLevel.MEMORY_AND_DISK());
            userRdds.put(source.concat("-").concat(target), union);
        }
    }

    //获取对象数组Rdd 每列都有类型对应
    private JavaRDD<Object[]> getObjectArrayRDD(List<Map<String, String>> maps,
                                                JavaPairRDD<LongWritable, Text> records, String separator) {
        String[] ints = new String[maps.size()];
        for (int i = 0; i < maps.size(); i++) {
            int finali = i;
            for (int j = 0; j < maps.size(); j++) {
                if (String.valueOf(finali + 1).equals(maps.get(j).get("columnid"))) {
                    ints[finali] = maps.get(j).get("columntype");
                }
            }
        }
        JavaRDD<Object[]> map = records.map(r -> {
            String[] split = r._2().toString().split(separator);
            Object[] object = new Object[split.length];
            for (int i = 0; i < split.length; i++) {
                object[i] = typeConversion(split[i], ints[i]);
            }
            return object;
        }).persist(StorageLevel.MEMORY_AND_DISK());
        return map;
    }

    public Object typeConversion(String column, String columnType) {
        switch (columnType) {
            case "1":
                return Integer.parseInt(column);
            case "2":
                return String.valueOf(column);
            case "3":
                return Boolean.valueOf(column);
            case "4":
                return Double.valueOf(column);
            case "5":
                return String.valueOf(column);
            default:
                return column;
        }
    }

    @Override
    public <T> T streaming(Map<String, Object> var1, KafkaStreaming var2) throws Exception {
        return null;
    }

}
