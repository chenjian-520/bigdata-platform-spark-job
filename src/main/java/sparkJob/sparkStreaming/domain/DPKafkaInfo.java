package sparkJob.sparkStreaming.domain;

import com.alibaba.fastjson.JSONObject;
import scala.Serializable;

public class DPKafkaInfo implements Serializable {
    private String serverUrl;
    private String topics;
    private String groupId;
    private Long batchDuration;
    private Long windowDurationMultiple;
    private Long sliverDurationMultiple;
    private JSONObject kafkaConf;

    public DPKafkaInfo() {
    }

    public String getTopics() {
        return this.topics;
    }

    public void setTopics(String topics) {
        this.topics = topics;
    }

    public String getServerUrl() {
        return this.serverUrl;
    }

    public void setServerUrl(String serverUrl) {
        this.serverUrl = serverUrl;
    }

    public String getGroupId() {
        return this.groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public Long getBatchDuration() {
        return this.batchDuration;
    }

    public void setBatchDuration(Long batchDuration) {
        this.batchDuration = batchDuration;
    }

    public Long getWindowDurationMultiple() {
        return this.windowDurationMultiple;
    }

    public void setWindowDurationMultiple(Long windowDurationMultiple) {
        this.windowDurationMultiple = windowDurationMultiple;
    }

    public Long getSliverDurationMultiple() {
        return this.sliverDurationMultiple;
    }

    public void setSliverDurationMultiple(Long sliverDurationMultiple) {
        this.sliverDurationMultiple = sliverDurationMultiple;
    }

    public JSONObject getKafkaConf() {
        return this.kafkaConf;
    }

    public void setKafkaConf(JSONObject kafkaConf) {
        this.kafkaConf = kafkaConf;
    }
}
