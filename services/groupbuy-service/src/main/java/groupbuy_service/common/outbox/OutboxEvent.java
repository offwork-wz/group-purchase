package groupbuy_service.common.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "tb_outbox_event")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class OutboxEvent {
    public enum Status { PENDING, PUBLISHED }

    @Id
    @Column(name = "outbox_id", length = 36)
    private String outboxId;

    @Column(nullable = false, length = 100)
    private String topic;

    @Column(name = "message_key", nullable = false, length = 100)
    private String messageKey;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private Status status;

    @Column(nullable = false)
    private int attempts;

    @Column(name = "next_attempt_at", nullable = false)
    private Instant nextAttemptAt;

    @Column(name = "last_error", columnDefinition = "TEXT")
    private String lastError;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    public OutboxEvent(String topic, String messageKey, String payload) {
        this.outboxId = UUID.randomUUID().toString();
        this.topic = topic;
        this.messageKey = messageKey;
        this.payload = payload;
        this.status = Status.PENDING;
        this.createdAt = Instant.now();
        this.nextAttemptAt = createdAt;
    }

    public void markPublished() {
        attempts++;
        status = Status.PUBLISHED;
        publishedAt = Instant.now();
        lastError = null;
    }

    public void scheduleRetry(String errorMessage) {
        attempts++;
        lastError = errorMessage;
        long delaySeconds = Math.min(60L, 1L << Math.min(attempts, 6));
        nextAttemptAt = Instant.now().plusSeconds(delaySeconds);
    }
}
