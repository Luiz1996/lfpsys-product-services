package com.lfpsys.lfpsys_product_services.kafka.consumers;

import static com.lfpsys.lfpsys_product_services.kafka.KafkaConfig.DLT_TOPIC_NAME;
import static com.lfpsys.lfpsys_product_services.nfe_upload.NfeUploadProcessStatus.ERROR;
import static com.lfpsys.lfpsys_product_services.nfe_upload.NfeUploadProcessType.INSERT_PRODUCTS;
import static java.lang.String.format;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lfpsys.lfpsys_product_services.nfe_upload.NfeUploadProcessType;
import com.lfpsys.lfpsys_product_services.nfe_upload.NfeUploadStatusDto;
import java.time.Duration;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ProductsDltConsumer {

  private static final Logger logger = LoggerFactory.getLogger(ProductsDltConsumer.class);
  private static final String REDIS_KEY_PREFIX = "LFPSys:NFe_Upload:%s";

  private final RedisTemplate<String, String> redisTemplate;
  private final ObjectMapper objectMapper;

  public ProductsDltConsumer(final RedisTemplate<String, String> redisTemplate, final ObjectMapper objectMapper) {
    this.redisTemplate = redisTemplate;
    this.objectMapper = objectMapper;
  }

  @KafkaListener(topics = DLT_TOPIC_NAME, groupId = "group_id")
  public void consumeMessage(ConsumerRecord<String, String> consumerRecord) {
    try {
      final var redisKey = format(REDIS_KEY_PREFIX, UUID.fromString(consumerRecord.key()));
      final var status = objectMapper.readValue(redisTemplate.opsForValue().get(redisKey), NfeUploadStatusDto.class);

      status
          .getProcesses()
          .forEach(nfeUploadProcess -> {
            if (INSERT_PRODUCTS.equals(nfeUploadProcess.getProcess())) {
              nfeUploadProcess.setStatus(ERROR);
            }
          });

      redisTemplate.opsForValue().set(redisKey, objectMapper.writeValueAsString(status), Duration.ofDays(30));
    } catch (Exception ex) {
      logger.error("Error: {}", ex.getMessage());
    }
  }
}
