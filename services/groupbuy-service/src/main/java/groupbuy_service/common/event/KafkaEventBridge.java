package groupbuy_service.common.event;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import groupbuy_service.common.outbox.OutboxEvent;
import groupbuy_service.common.outbox.OutboxEventRepository;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaEventBridge {

    private final OutboxEventRepository outboxEventRepository;
    private final JsonMapper jsonMapper;

    @TransactionalEventListener(phase = TransactionPhase.BEFORE_COMMIT)
    @Transactional(propagation = Propagation.MANDATORY)
    public void handleDomainEvent(DomainEvent event) {
        String payload = jsonMapper.writeValueAsString(event);
        outboxEventRepository.save(new OutboxEvent(event.getTopic(), event.getKey(), payload));
        log.debug("Outbox event saved in domain transaction: topic={}, key={}", event.getTopic(), event.getKey());
    }
}
