package groupbuy_service.global.reconciliation.service;

import groupbuy_service.global.reconciliation.FailedEvent;
import groupbuy_service.global.reconciliation.FailedEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

@Slf4j
@Service
@RequiredArgsConstructor
public class DltReconciliationService {

    private final FailedEventRepository failedEventRepository;
    private final JsonMapper jsonMapper;
    private static final String FALLBACK_PAYLOAD = "Serialization failed";

    @Transactional
    public void logFailedEvent(
            String topic,
            Integer originalPartition,
            Long originalOffset,
            Object payload,
            String errorMessage
    ) {
        if (originalPartition == null || originalOffset == null) {
            log.warn(
                    "DLT original metadata missing; deduplication unavailable. topic={}, partition={}, offset={}",
                    topic, originalPartition, originalOffset
            );
        }

        String jsonPayload = serializePayload(payload);

        FailedEvent failedEvent = FailedEvent.builder()
                .topic(topic)
                .originalPartition(originalPartition)
                .originalOffset(originalOffset)
                .payload(jsonPayload)
                .errorMessage(errorMessage)
                .build();

        int insertedRows = failedEventRepository.insertIgnoringDuplicate(
                failedEvent.getFailedEventId(),
                failedEvent.getTopic(),
                failedEvent.getOriginalPartition(),
                failedEvent.getOriginalOffset(),
                failedEvent.getPayload(),
                failedEvent.getErrorMessage(),
                failedEvent.getCreatedAt()
        );

        if (insertedRows == 0) {
            log.info("DLT failed event already recorded. topic={}, partition={}, offset={}",
                    topic, originalPartition, originalOffset);
        }
    }

    private String serializePayload(Object payload) {
        String jsonPayload = FALLBACK_PAYLOAD;
        try {
            jsonPayload = jsonMapper.writeValueAsString(payload);
        }catch (JacksonException e) {
            log.error("DLT payload Jackson Serialization failed",e);
        }

        return jsonPayload;
    }

}
