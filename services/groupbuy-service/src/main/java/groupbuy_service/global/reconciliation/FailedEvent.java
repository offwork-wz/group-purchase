package groupbuy_service.global.reconciliation;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(
        name = "tb_failed_event",
        uniqueConstraints = @UniqueConstraint(
                name = "uq_failed_event_original_record",
                columnNames = {"topic", "original_partition", "original_offset"}
        )
)
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class FailedEvent {

    @Id
    @Column(name = "failed_event_id", length = 50)
    private String failedEventId;

    @Column(nullable = false, length = 100)
    private String topic;

    @Column(name = "original_partition")
    private Integer originalPartition;

    @Column(name = "original_offset")
    private Long originalOffset;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Column(columnDefinition = "TEXT")
    private String errorMessage;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    @Builder
    public FailedEvent(String topic, Integer originalPartition, Long originalOffset, String payload, String errorMessage) {
        this.failedEventId = UUID.randomUUID().toString();
        this.topic = topic;
        this.originalPartition = originalPartition;
        this.originalOffset = originalOffset;
        this.payload = payload;
        this.errorMessage = errorMessage;
        this.createdAt = Instant.now();
    }


}
