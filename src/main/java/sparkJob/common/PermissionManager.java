package sparkJob.common;

import org.apache.hadoop.conf.Configuration;
import sparkJob.mysql.entity.DBConnectionInfo;
import sparkJob.sparkStreaming.domain.DPKafkaInfo;

public interface PermissionManager {

    DBConnectionInfo getMysqlInfo();

    String getRootHdfsUri();

    Configuration initialHdfsSecurityContext();

    DPKafkaInfo initialKafkaSecurityContext();
}

