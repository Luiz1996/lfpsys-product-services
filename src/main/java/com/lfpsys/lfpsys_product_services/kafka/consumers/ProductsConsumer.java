package com.lfpsys.lfpsys_product_services.kafka.consumers;

import static com.lfpsys.lfpsys_product_services.kafka.KafkaConfig.TOPIC_NAME;
import static com.lfpsys.lfpsys_product_services.nfe_upload.NfeUploadProcessStatus.COMPLETED;
import static com.lfpsys.lfpsys_product_services.nfe_upload.NfeUploadProcessType.INSERT_PRODUCTS;
import static com.lfpsys.lfpsys_product_services.nfe_upload.NfeUploadProcessType.NFE_UPLOAD;
import static java.lang.String.format;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lfpsys.lfpsys_product_services.PendingBeforeStepException;
import com.lfpsys.lfpsys_product_services.nfe_upload.NfeUploadProcessType;
import com.lfpsys.lfpsys_product_services.nfe_upload.NfeUploadStatusDto;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ProductsConsumer {

  private static final String REDIS_KEY_PREFIX = "LFPSys:NFe_Upload:%s";

  private final RedisTemplate<String, String> redisTemplate;
  private final ObjectMapper objectMapper;

  public ProductsConsumer(final RedisTemplate<String, String> redisTemplate, final ObjectMapper objectMapper) {
    this.redisTemplate = redisTemplate;
    this.objectMapper = objectMapper;
  }

  @KafkaListener(topics = TOPIC_NAME, groupId = "group_id")
  public void consumeMessage(ConsumerRecord<String, String> consumerRecord) throws JsonProcessingException {
    final var redisKey = format(REDIS_KEY_PREFIX, UUID.fromString(consumerRecord.key()));
    final var status = objectMapper.readValue(redisTemplate.opsForValue().get(redisKey), NfeUploadStatusDto.class);

    final var beforeStepIsCompleted = status
        .getProcesses()
        .stream()
        .anyMatch(process -> NFE_UPLOAD.equals(process.getProcess()) && COMPLETED.equals(process.getStatus()));

    if (!beforeStepIsCompleted) {
      throw new PendingBeforeStepException();
    }

    status
        .getProcesses()
        .forEach(nfeUploadProcess -> {
          if (INSERT_PRODUCTS.equals(nfeUploadProcess.getProcess())) {
            nfeUploadProcess.setStatus(COMPLETED);
          }
        });

    redisTemplate.opsForValue().set(redisKey, objectMapper.writeValueAsString(status));
  }
}
