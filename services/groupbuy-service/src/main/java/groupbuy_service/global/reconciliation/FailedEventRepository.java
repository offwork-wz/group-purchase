package groupbuy_service.global.reconciliation;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;

@Repository
public interface FailedEventRepository extends JpaRepository<FailedEvent, String> {
    @Modifying
    @Query(value = """
            INSERT INTO tb_failed_event (
                failed_event_id, topic, original_partition, original_offset,
                payload, error_message, created_at
            ) VALUES (
                :failedEventId, :topic, :originalPartition, :originalOffset,
                :payload, :errorMessage, :createdAt
            )
            ON CONFLICT (topic, original_partition, original_offset) DO NOTHING
            """, nativeQuery = true)
    int insertIgnoringDuplicate(
            @Param("failedEventId") String failedEventId,
            @Param("topic") String topic,
            @Param("originalPartition") Integer originalPartition,
            @Param("originalOffset") Long originalOffset,
            @Param("payload") String payload,
            @Param("errorMessage") String errorMessage,
            @Param("createdAt") Instant createdAt
    );
}
