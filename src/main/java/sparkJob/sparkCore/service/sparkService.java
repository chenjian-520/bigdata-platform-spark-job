package sparkJob.sparkCore.service;


import scala.Serializable;
import sparkJob.sparkStreaming.KafkaStreaming;

import java.util.Map;

/**
 * sparkJob 实现业务逻辑继承此接口
 */
public interface SparkService extends Serializable {

    <T> T execute(Map<String, Object> var) throws Exception;

    <T> T streaming(Map<String, Object> var, KafkaStreaming kafkaStreaming) throws Exception;
}

