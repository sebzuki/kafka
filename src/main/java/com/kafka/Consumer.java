package com.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import static org.springframework.kafka.retrytopic.DltStrategy.FAIL_ON_ERROR;
import static org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy.SINGLE_TOPIC;

@Component
@RequiredArgsConstructor
@Log4j2
public class Consumer {


    /**
     * RESULTATS au niveaux des topics <br>
     * ----------------------- <br>
     * Topics
     *     TOPIC <br>
     *     TOPIC-retry <br>
     *     TOPIC-dlt <br>
     * ------------------------ <br>
     * Consumer Groups associés (attention ils doivent être différents sinon il y aura mix des offsets, ils sont générés auto) <br>
     *     testseb <br>
     *     testseb-retry <br>
     *     testseb-dlt <br>
     */
    @KafkaListener(
            topics = "${consumer.topic}",
            groupId = "${consumer.groupId}",
            containerFactory = "kafkaListenerContainerFactory")
    @RetryableTopic(
            attempts = "10",
            backoff = @Backoff(delay = 20000),
            sameIntervalTopicReuseStrategy = SINGLE_TOPIC,
            exclude = {DeserializationException.class}, // les exceptions à ne pas rejouer et à envoyer en dead letter topic immediatement
            include = {DataAccessException.class}, // les exceptions à rejouer
            dltStrategy = FAIL_ON_ERROR
            //dltStrategy = NO_DLT, // si on ne veut pas de dead letter queue le message est jeté après les retries
            )
    public void listen(ConsumerRecord<String, String> message) {
        try {
            processError(message.value());
        } catch (Exception ex) {
            log.warn("Fail to handle event {}.", message);
            throw ex;
        }
    }

    private void process(String message) {
        log.info("Received message : " + message);
    }

    /**
     * Pour customiser le message d'erreur en cas de fail conso
     */
    @DltHandler
    public void processMessage(String message) {
        log.error("DltHandler processMessage = {}", message);
    }

    private void processError(String message) {
        log.error("processError = {}", message);
        throw new IllegalArgumentException("processError = " + message);
    }
}
