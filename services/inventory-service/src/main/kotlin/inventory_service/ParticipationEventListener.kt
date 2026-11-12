package inventory_service.event

import inventory_service.domain.StockDeductionHistory
import inventory_service.domain.StockDeductionStatus
import inventory_service.global.reconciliation.DltReconciliationService
import inventory_service.service.InventoryService
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.BackOff
import org.springframework.kafka.annotation.DltHandler
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.annotation.RetryableTopic
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.retrytopic.DltStrategy
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.stereotype.Component
import tools.jackson.core.JacksonException
import tools.jackson.databind.json.JsonMapper

@Component
class ParticipationEventListener(
    private val jsonMapper: JsonMapper,
    private val inventoryService: InventoryService,
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val dltReconciliationService: DltReconciliationService
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @RetryableTopic(
        attempts = "3",
        backOff = BackOff(delay = 1000, multiplier = 2.0, maxDelay = 20000),
        exclude = [JacksonException::class],
        dltStrategy = DltStrategy.FAIL_ON_ERROR,
        autoCreateTopics = "true"
    )
    @KafkaListener(topics = [ParticipationRequestedEvent.TOPIC], groupId = "inventory-service-group")
    fun onParticipationRequested(event: ParticipationRequestedEvent) {
        val history = inventoryService.deductStockForParticipation(
            event.participationId, event.groupbuyId, event.productId, event.userId, event.quantity
        )
        // 서비스 트랜잭션이 커밋된 결과만 발행한다. 중복도 저장된 원본 결과를 사용한다.
        when (history.status) {
            StockDeductionStatus.SUCCESS -> publishSuccessEvent(history)
            StockDeductionStatus.FAILED -> publishFailureEvent(history)
            StockDeductionStatus.PROCESSING -> error("미완료 재고 차감 결과. participationId=${history.participationId}")
        }
    }

    @DltHandler
    fun handleDlt(
        event: ParticipationRequestedEvent,
        @Header(value = KafkaHeaders.ORIGINAL_TOPIC, required = false) originalTopic: String?,
        @Header(value = KafkaHeaders.ORIGINAL_PARTITION, required = false) originalPartition: Int?,
        @Header(value = KafkaHeaders.ORIGINAL_OFFSET, required = false) originalOffset: Long?,
        @Header(value = KafkaHeaders.EXCEPTION_MESSAGE, required = false) exceptionMessage: String?
    ) {
        val topic = originalTopic ?: ParticipationRequestedEvent.TOPIC
        val errorMessage = exceptionMessage ?: "FailedEvent: ParticipationRequestedEvent"

        log.error("[DLT] 참여 요청 처리 최종 실패. topic={}, error={}, content={}", topic, errorMessage, event)
        dltReconciliationService.logFailedEvent(topic, originalPartition, originalOffset, event, errorMessage)
    }

    private fun publishSuccessEvent(history: StockDeductionHistory) {
        val successEvent = StockDecreasedEvent(
            eventId = history.resultEventId,
            occurredAt = history.createdAt,
            participationId = history.participationId,
            productId = history.productId,
            quantity = history.quantity
        )
        val payload = jsonMapper.writeValueAsString(successEvent)
        kafkaTemplate.send(StockDecreasedEvent.TOPIC, history.participationId, payload)
    }

    private fun publishFailureEvent(history: StockDeductionHistory) {
        val failureEvent = StockDecreaseFailedEvent(
            eventId = history.resultEventId,
            occurredAt = history.createdAt,
            participationId = history.participationId,
            productId = history.productId,
            quantity = history.quantity,
            errorCode = checkNotNull(history.errorCode).name,
            errorMessage = checkNotNull(history.errorMessage)
        )
        val payload = jsonMapper.writeValueAsString(failureEvent)
        kafkaTemplate.send(StockDecreaseFailedEvent.TOPIC, history.participationId, payload)
    }

}
