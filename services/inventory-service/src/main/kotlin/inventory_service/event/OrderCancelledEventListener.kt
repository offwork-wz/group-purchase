package inventory_service.event

import inventory_service.global.reconciliation.DltReconciliationService
import inventory_service.service.InventoryService
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.kafka.annotation.BackOff
import org.springframework.kafka.annotation.DltHandler
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.annotation.RetryableTopic
import org.springframework.kafka.retrytopic.DltStrategy
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.stereotype.Component
import tools.jackson.core.JacksonException

@Component
class OrderCancelledEventListener(
    private val inventoryService: InventoryService,
    private val dltReconciliationService: DltReconciliationService
) {
    private val log = KotlinLogging.logger {}

    @RetryableTopic(
        attempts = "3",
        backOff = BackOff(delay = 1000, multiplier = 2.0, maxDelay = 20000),
        exclude = [JacksonException::class],
        dltStrategy = DltStrategy.FAIL_ON_ERROR,
        autoCreateTopics = "true"
    )
    @KafkaListener(topics = [OrderCancelledEvent.TOPIC], groupId = "inventory-service-order-cancelled")
    fun restoreStock(event: OrderCancelledEvent) {
        inventoryService.restoreStockForOrder(event.orderId, event.productId, event.quantity)
    }

    @DltHandler
    fun handleDlt(
        event: OrderCancelledEvent,
        @Header(value = KafkaHeaders.ORIGINAL_TOPIC, required = false) originalTopic: String?,
        @Header(value = KafkaHeaders.ORIGINAL_PARTITION, required = false) originalPartition: Int?,
        @Header(value = KafkaHeaders.ORIGINAL_OFFSET, required = false) originalOffset: Long?,
        @Header(value = KafkaHeaders.EXCEPTION_MESSAGE, required = false) exceptionMessage: String?
    ) {
        val topic = originalTopic ?: OrderCancelledEvent.TOPIC
        val errorMessage = exceptionMessage ?: "FailedEvent: OrderCancelledEvent"

        log.error { "[DLT] 주문취소 재고 처리 최종 실패. topic=$topic, error=$errorMessage, content=$event" }
        dltReconciliationService.logFailedEvent(topic, originalPartition, originalOffset, event, errorMessage)
    }
}
