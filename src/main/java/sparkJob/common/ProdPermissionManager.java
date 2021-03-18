package sparkJob.common;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sparkJob.SparkApp;
import sparkJob.mysql.entity.DBConnectionInfo;
import sparkJob.sparkStreaming.domain.DPKafkaInfo;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;


public class ProdPermissionManager implements PermissionManager, Serializable {

    private static Properties prop = new Properties();
    private static final Logger logger = LoggerFactory.getLogger(ProdPermissionManager.class);

    public ProdPermissionManager() {
    }

    static {
        init();
    }

    private static void init() {
        try (InputStream propFile = ProdPermissionManager.class.getResource("../../ProPermissionManager.properties").openStream()) {
            prop.load(new InputStreamReader(propFile, StandardCharsets.UTF_8));
        } catch (IOException e) {
            logger.error("ProPermissionManager init exception");
        }
    }

    public DBConnectionInfo getMysqlInfo() {
        DBConnectionInfo dbConnectionInfo = new DBConnectionInfo();
        dbConnectionInfo.setUrl(prop.getProperty("mysqlUrl"));
        dbConnectionInfo.setUsername(prop.getProperty("mysqlUsername"));
        dbConnectionInfo.setPassword(prop.getProperty("mysqlPassword"));
        return dbConnectionInfo;
    }

    public DBConnectionInfo getSqlserverInfo() {
        DBConnectionInfo dbConnectionInfo = new DBConnectionInfo();
        dbConnectionInfo.setUrl(prop.getProperty("serverSqlUrl"));
        dbConnectionInfo.setUsername(prop.getProperty("serverUsername"));
        dbConnectionInfo.setPassword(prop.getProperty("serverPassword"));
        return dbConnectionInfo;
    }

    public String getRootHdfsUri() {
        return prop.getProperty("hdfsUri");
    }

    public Configuration initialHdfsSecurityContext() {
        Configuration config = new Configuration();
        return config;
    }

    @Override
    public DPKafkaInfo initialKafkaSecurityContext() {
        DPKafkaInfo dpKafkaInfo = SparkApp.getDPKafkaInfo();
        dpKafkaInfo.setServerUrl(prop.getProperty("kafkaServerUrl"));
        return dpKafkaInfo;
    }
}
