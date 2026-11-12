package groupbuy_service.common.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Service
@RequiredArgsConstructor
public class OutboxPublisher {
    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    // 스케줄러가 주입받은 프록시를 호출한다. 이벤트 한 건마다 독립적인 트랜잭션이다.
    @Transactional
    public boolean publishNext() {
        OutboxEvent event = outboxEventRepository.findNextForUpdate().orElse(null);
        if (event == null) {
            return false;
        }

        try {
            kafkaTemplate.send(event.getTopic(), event.getMessageKey(), event.getPayload())
                    .get(12, TimeUnit.SECONDS);
            event.markPublished();
            log.debug("Outbox published: id={}, topic={}", event.getOutboxId(), event.getTopic());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Outbox publishing interrupted: " + event.getOutboxId(), e);
        } catch (KafkaException | org.apache.kafka.common.KafkaException | ExecutionException | TimeoutException e) {
            event.scheduleRetry(e.toString());
            log.warn("Outbox publish failed; kept PENDING: id={}, topic={}, attempts={}, nextAttemptAt={}",
                    event.getOutboxId(), event.getTopic(), event.getAttempts(), event.getNextAttemptAt(), e);
        }
        return true;
    }
}
