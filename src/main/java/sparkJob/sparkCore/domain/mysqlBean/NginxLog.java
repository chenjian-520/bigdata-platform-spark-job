package sparkJob.sparkCore.domain.mysqlBean;

import lombok.Data;
import scala.Serializable;

@Data
public class NginxLog implements Serializable {
    private String remoteAddr;
    private String httpXForwardedFor;
    private String timeLocal;
    private long status;
    private long bodyBytesSent;
    private String httpUserAgent;
    private String httpReferer;
    private String requestMethod;
    private String requestTime;
    private String requestUri;
    private String serverProtocol;
    private String requestBody;
    private String httpToken;
}