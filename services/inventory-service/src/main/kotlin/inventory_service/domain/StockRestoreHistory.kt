package inventory_service.domain

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.Instant

@Entity
@Table(name = "stock_restore_history")
class StockRestoreHistory(
    @Id
    @Column(name = "order_id", length = 36)
    val orderId: String,

    @Column(name = "product_id", nullable = false, length = 36)
    val productId: String,

    @Column(nullable = false)
    val quantity: Int,

    @Column(name = "created_at", nullable = false, updatable = false)
    val createdAt: Instant = Instant.now()
) {
    protected constructor() : this(orderId = "", productId = "", quantity = 0)
}
