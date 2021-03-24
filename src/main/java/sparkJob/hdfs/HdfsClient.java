package sparkJob.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sparkJob.SparkApp;
import sparkJob.common.PermissionManager;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ResourceBundle;

public final class HdfsClient {
    /**
     * 日志对象
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsClient.class);

    /**
     * username 认证principalkey
     */
    private static String PRINCIPAL = "username.client.kerberos.principal";

    /**
     * username 认证keytab.key
     */
    private static String KEYTAB = "username.client.keytab.file";

    /**
     * 单例
     */
    private static final HdfsClient single = new HdfsClient();

    /**
     * HDFS 配置
     */
    private static Configuration conf;

    private static FileSystem fileSystem = null;
    /**
     * spark 属性配置
     */
    private static ResourceBundle resourceBundle = ResourceBundle.getBundle("hdfs-client");

    /**
     * 私有化构造器
     */
    private HdfsClient() {
    }

    /**
     * 获取实例
     */
    public static HdfsClient getInstance() {
        return single;
    }

    static {
        init();
    }

    /**
     * 初始化
     */
    private static void init() {
        conf = new Configuration();
        conf.addResource(resourceBundle.getString("hdfsSitePath"));
        conf.addResource(resourceBundle.getString("coreSitePath"));
        try {
            initHDFSFileSystem();
        } catch (IOException e) {
            LOGGER.error("hdfs-fileSystem init  error! ");
        }
    }

    public static boolean deleteFile(String path) throws IOException {
        return fileSystem.delete(new Path(path), false);
    }

    /**
     * 初始化hdfs文件系统，返回默认文件系统，core-site.xml中指定的，如果没有指定，则默认本地文件系统
     *
     * @throws IOException
     */
    public static void initHDFSFileSystem() throws IOException {
        Configuration config = new Configuration();
        fileSystem = FileSystem.get(config);
    }

    /**
     * 初始化hdfs文件系统作为给定用户来访问文件系统
     *
     * @param user Linux用户名
     * @throws URISyntaxException   HDFS地址
     * @throws IOException          配置信息
     * @throws InterruptedException
     */
    public static void initHDFSFileSystem(String user) throws URISyntaxException, IOException, InterruptedException {
        PermissionManager pm = SparkApp.getDpPermissionManager();
        fileSystem = FileSystem.get(new URI(pm.getRootHdfsUri()), pm.initialHdfsSecurityContext(), user);
    }

    /**
     * 删除存在路径文件
     *
     * @param filepath
     * @throws IOException
     */
    public static void deletefilepath(String filepath) throws IOException {
        String spath = filepath;
        Path path = new Path(spath);
        if (fileSystem.exists(path)) {
            FileStatus[] files = fileSystem.listStatus(path);
            for (FileStatus file : files) {
                fileSystem.delete(file.getPath(), false);
            }
            fileSystem.delete(path, true);
        }
        fileSystem.close();
    }

    /**
     * 获取hdfs文件返回JavaRDD
     *
     * @param path
     * @return
     */
    public static JavaPairRDD<LongWritable, Text> readHdfsNewApi(String path) {
        JobConf jobConf = new JobConf();
        FileInputFormat.setInputPaths(jobConf, new Path(path));
        Configuration config = new Configuration();
        JavaPairRDD<LongWritable, Text> longWritableTextJavaPairRDD = SparkApp.contextBroadCast.value().get(0)
                .newAPIHadoopFile(path, CombineTextInputFormat.class, LongWritable.class, Text.class, config);

        return longWritableTextJavaPairRDD;
    }

    /**
     * 备用方法
     * @param path
     * @param partitions
     * @return
     */
    /*
    public static RDD<Tuple2<LongWritable,Text>> readHdfsRDD(String path,int partitions){
        JobConf jobConf = new JobConf();
        FileInputFormat.setInputPaths(jobConf,new Path(path));
        RDD<Tuple2<LongWritable, Text>> tuple2RDD = SparkApp.sessionBroadcast.value().get(0)
                .sparkContext()
                .hadoopRDD(jobConf, TextInputFormat.class, LongWritable.class, Text.class, partitions);
        return tuple2RDD;
    }
    */

    /**
     * 上传hdfs
     * 注意：如果上传的内容大于128MB,则是2块
     */
    public static boolean putFileToHDFS(String user, String sysPath, String dfsPath) throws Exception {

        try {
            initHDFSFileSystem(user);
            //上传本地文件的路径
            Path src = new Path(sysPath);
            //要上传到HDFS的路径
            Path dst = new Path(dfsPath);
            //以拷贝的方式上传，
            fileSystem.copyFromLocalFile(src, dst);
            fileSystem.close();
            LOGGER.info("上传成功");
            return true;
        } catch (URISyntaxException e) {
            System.out.println(e.getMessage());
            return false;
        } catch (IOException e) {
            System.out.println(e.getMessage());
            return false;
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
            return false;
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            return false;
        }
    }

    /**
     * 下载hdfs
     * hadoop fs -get /HDFS文件系统
     */
    public void getFileFromHDFS(String user, String oldDfsPath, String dfsPath) throws Exception {
        initHDFSFileSystem(user);

        //下载文件
        //boolean delSrc:是否将原文件删除
        //Path oldDfsPath ：要下载的路径
        //Path dfsPath ：要下载到哪
        //boolean useRawLocalFileSystem ：是否校验文件
        fileSystem.copyToLocalFile(false, new Path(oldDfsPath),
                new Path(dfsPath), true);
        fileSystem.close();
        System.out.println("下载成功");
    }

    /**
     * 下载HDFS文件输出到outputstream
     */
    public void downLoadHdfsFile(String path, String fileName, OutputStream outputStream) throws IOException {
        /**
         * 日志输出
         */
        LOGGER.info("download start,path={},fileName={}", path, fileName);

        /**
         * 认证
         */
        authentication();
        /**
         * 获取HDFS文件系统
         */
        FileSystem fileSystem = FileSystem.get(conf);
        /**
         * 获取文件path
         */
        Path filePath = new Path(path, fileName);
        try (FSDataInputStream fsDataInputStream = fileSystem.open(filePath);) {
            byte[] buff = new byte[10];
            int length = -1;
            while (fsDataInputStream.read(buff) != -1) {
                outputStream.write(buff, 0, length);
            }
            LOGGER.info("download finish,path={},fileName={}", path, fileName);
        }
    }

    /**
     * 用户认证
     */
    private void authentication() throws IOException {
        if ("kerberis".equalsIgnoreCase(conf.get("hadoop.security.authentication"))) {
            conf.set(PRINCIPAL, resourceBundle.getString("principal"));
            conf.set(KEYTAB, resourceBundle.getString("keytab"));
            System.setProperty("java.security.krb5.conf", resourceBundle.getString("krb5"));
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(conf.get(PRINCIPAL), conf.get(KEYTAB));
        }
    }
}