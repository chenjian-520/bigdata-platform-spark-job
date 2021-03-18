package sparkJob.sparkCore.service;


import scala.Serializable;
import sparkJob.sparkStreaming.KafkaStreaming;

import java.util.Map;

public interface sparkService extends Serializable {

    <T> T execute(Map<String, Object> var) throws Exception;

    <T> T streaming(Map<String, Object> var, KafkaStreaming kafkaStreaming) throws Exception;
}

