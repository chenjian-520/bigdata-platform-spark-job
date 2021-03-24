package sparkJob.common;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sparkJob.SparkApp;
import sparkJob.mysql.entity.DBConnectionInfo;
import sparkJob.sparkStreaming.domain.DPKafkaInfo;
import java.io.Serializable;
import java.util.ResourceBundle;

/**
 * 配置类
 */
public class ProdPermissionManager implements PermissionManager, Serializable {

    private static ResourceBundle resourceBundle = ResourceBundle.getBundle("ProPermissionManager");

    public ProdPermissionManager() {
    }

    public DBConnectionInfo getMysqlInfo() {
        DBConnectionInfo dbConnectionInfo = new DBConnectionInfo();
        dbConnectionInfo.setUrl(resourceBundle.getString("mysqlUrl"));
        dbConnectionInfo.setUsername(resourceBundle.getString("mysqlUsername"));
        dbConnectionInfo.setPassword(resourceBundle.getString("mysqlPassword"));
        return dbConnectionInfo;
    }

    public DBConnectionInfo getSqlserverInfo() {
        DBConnectionInfo dbConnectionInfo = new DBConnectionInfo();
        dbConnectionInfo.setUrl(resourceBundle.getString("serverSqlUrl"));
        dbConnectionInfo.setUsername(resourceBundle.getString("serverUsername"));
        dbConnectionInfo.setPassword(resourceBundle.getString("serverPassword"));
        return dbConnectionInfo;
    }

    public String getRootHdfsUri() {
        return resourceBundle.getString("hdfsUri");
    }

    public Configuration initialHdfsSecurityContext() {
        Configuration config = new Configuration();
        return config;
    }

    @Override
    public DPKafkaInfo initialKafkaSecurityContext() {
        DPKafkaInfo dpKafkaInfo = SparkApp.getDPKafkaInfo();
        dpKafkaInfo.setServerUrl(resourceBundle.getString("kafkaServerUrl"));
        return dpKafkaInfo;
    }
}
