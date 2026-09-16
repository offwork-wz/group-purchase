package groupbuy_service.participation.event;

import groupbuy_service.global.reconciliation.service.DltReconciliationService;
import groupbuy_service.order.event.OrderCancelledEvent;
import groupbuy_service.participation.service.ParticipationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderCancelledEventListener {

    private final ParticipationService participationService;
    private final DltReconciliationService dltReconciliationService;

    @RetryableTopic(
            attempts = "3",
            backOff = @BackOff(delay = 1000, multiplier = 2.0, maxDelay = 20000),
            exclude = {JacksonException.class},
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            autoCreateTopics = "true"
    )
    @KafkaListener(topics = OrderCancelledEvent.TOPIC, groupId = "groupbuy-service-group")
    public void onOrderCancelledEvent(OrderCancelledEvent event) throws Exception {
        log.info("[groupbuy-service] 주문 취소 이벤트 수신: participationId={}", event.participationId());
        participationService.failParticipation(event.participationId());
    }

    @DltHandler
    public void handleDlt(
            OrderCancelledEvent event,
            @Header(value = KafkaHeaders.ORIGINAL_TOPIC, required = false) String originalTopic,
            @Header(value = KafkaHeaders.ORIGINAL_PARTITION, required = false) Integer originalPartition,
            @Header(value = KafkaHeaders.ORIGINAL_OFFSET, required = false) Long originalOffset,
            @Header(value = KafkaHeaders.EXCEPTION_MESSAGE, required = false) String exceptionMessage
    ) {
        String topic = originalTopic != null
                ? originalTopic
                : OrderCancelledEvent.TOPIC;
        String errorMessage = exceptionMessage != null
                ? exceptionMessage
                : "FailedEvent: OrderCancelledEvent";

        log.error("🚨 [DLT] 주문 취소 처리 최종 실패. topic: {}, event: {}, content: {}", topic, event, errorMessage);
        dltReconciliationService.logFailedEvent(topic, originalPartition, originalOffset, event, errorMessage);
    }
}
