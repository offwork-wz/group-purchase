package inventory_service.global.reconciliation

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import tools.jackson.core.JacksonException
import tools.jackson.databind.json.JsonMapper

@Service
class DltReconciliationService(
    private val failedEventRepository: FailedEventRepository,
    private val jsonMapper: JsonMapper
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Transactional
    fun logFailedEvent(
        topic: String,
        originalPartition: Int?,
        originalOffset: Long?,
        payload: Any,
        errorMessage: String
    ) {
        if (originalPartition == null || originalOffset == null) {
            log.warn(
                "DLT original metadata missing; deduplication unavailable. topic={}, partition={}, offset={}",
                topic,
                originalPartition,
                originalOffset
            )
        }

        val failedEvent = FailedEvent(
            topic = topic,
            originalPartition = originalPartition,
            originalOffset = originalOffset,
            payload = serializePayload(payload),
            errorMessage = errorMessage
        )

        val insertedRows = failedEventRepository.insertIgnoringDuplicate(
            failedEventId = failedEvent.failedEventId,
            topic = failedEvent.topic,
            originalPartition = failedEvent.originalPartition,
            originalOffset = failedEvent.originalOffset,
            payload = failedEvent.payload,
            errorMessage = failedEvent.errorMessage,
            createdAt = failedEvent.createdAt
        )

        if (insertedRows == 0) {
            log.info(
                "DLT failed event already recorded. topic={}, partition={}, offset={}",
                topic,
                originalPartition,
                originalOffset
            )
        }
    }

    private fun serializePayload(payload: Any): String =
        try {
            jsonMapper.writeValueAsString(payload)
        } catch (e: JacksonException) {
            log.error("DLT payload serialization failed", e)
            FALLBACK_PAYLOAD
        }

    private companion object {
        const val FALLBACK_PAYLOAD = "Serialization failed"
    }
}
