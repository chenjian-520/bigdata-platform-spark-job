package sparkJob.hdfs;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import sparkJob.SparkApp;

public class SystemFile {

    /**
     * 默认是从hdfs读取文件，也可以指定sc.textFile("路径").在路径前面加上hdfs://表示从hdfs文件系统上读
     * 本地文件读取 sc.textFile("路径").在路径前面加上file:// 表示从本地文件系统读，如file:///home/user/spark/README.md
     * 读取Text文件，使用readHdfsFile
     */
    public static JavaRDD<String> readSystemFile(String path, int partitionCount) {
        JavaSparkContext sparkContext = SparkApp.contextBroadCast.value().get(0);
        JavaRDD<String> txtData =
                sparkContext.textFile(path, partitionCount).persist(StorageLevel.MEMORY_AND_DISK());
        return txtData;
    }

    public static void saveSystemFile(JavaRDD rdd, String path) {
        SparkApp.getSession().sparkContext().hadoopConfiguration().
                set("mapreduce.fileoutputcommitter.marksuccessfuljobs","false");
        rdd.repartition(1).saveAsTextFile(path);
    }
}
