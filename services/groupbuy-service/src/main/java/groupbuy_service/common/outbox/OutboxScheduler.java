package groupbuy_service.common.outbox;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "outbox.publisher.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxScheduler {
    private final OutboxPublisher publisher;
    private final int batchSize;

    public OutboxScheduler(OutboxPublisher publisher, @Value("${outbox.publisher.batch-size:20}") int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("outbox.publisher.batch-size must be positive");
        }
        this.publisher = publisher;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${outbox.publisher.poll-interval-ms:1000}")
    public void publishPending() {
        for (int i = 0; i < batchSize; i++) {
            if (!publisher.publishNext()) {
                return;
            }
        }
    }
}
