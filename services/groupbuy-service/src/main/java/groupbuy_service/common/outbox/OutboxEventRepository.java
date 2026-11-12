package groupbuy_service.common.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import java.util.Optional;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, String> {
    @Query(value = """
            SELECT * FROM tb_outbox_event
            WHERE status = 'PENDING' AND next_attempt_at <= CURRENT_TIMESTAMP
            ORDER BY next_attempt_at, created_at, outbox_id
            LIMIT 1 FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    Optional<OutboxEvent> findNextForUpdate();
}
