package groupbuy_service.order.event;

import groupbuy_service.global.reconciliation.service.DltReconciliationService;
import groupbuy_service.order.service.OrderService;
import groupbuy_service.payment.domain.PaymentStatus;
import groupbuy_service.payment.event.PaymentCompletedEvent;
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
public class PaymentCompletedEventListener {

    private final OrderService orderService;
    private final DltReconciliationService dltReconciliationService;

    @RetryableTopic(
            attempts = "3",
            backOff = @BackOff(delay = 1000, multiplier = 2.0, maxDelay = 20000),
            exclude = {JacksonException.class},
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            autoCreateTopics = "true"
    )
    @KafkaListener(topics = PaymentCompletedEvent.TOPIC, groupId = "groupbuy-service-order")
    public void onPaymentCompleted(PaymentCompletedEvent event) throws Exception{
        log.info("결제 완료 이벤트 수신: orderId={}, status={}", event.orderId(), event.status());
        if (event.status() == PaymentStatus.SUCCESS) {
            orderService.completedOrder(event.orderId());
        }
        else if (event.status() == PaymentStatus.FAILED) {
            orderService.cancelOrder(event.orderId());
        }

    }

    @DltHandler
    public void handleDlt(
            PaymentCompletedEvent event,
            @Header(value = KafkaHeaders.ORIGINAL_TOPIC, required = false) String originalTopic,
            @Header(value = KafkaHeaders.ORIGINAL_PARTITION, required = false) Integer originalPartition,
            @Header(value = KafkaHeaders.ORIGINAL_OFFSET, required = false) Long originalOffset,
            @Header(value = KafkaHeaders.EXCEPTION_MESSAGE, required = false) String exceptionMessage
    ) {
        String topic = originalTopic != null
                ? originalTopic
                : PaymentCompletedEvent.TOPIC;
        String errorMessage = exceptionMessage != null
                ? exceptionMessage
                : "FailedEvent: PaymentCompletedEvent";

        log.error("결제 완료 처리 실패, topic: {}, event: {}, content: {}", topic, event, errorMessage);
        dltReconciliationService.logFailedEvent(topic, originalPartition, originalOffset, event, errorMessage);
    }

}
