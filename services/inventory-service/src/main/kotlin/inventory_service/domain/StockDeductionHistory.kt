package inventory_service.domain

import inventory_service.global.error.ErrorCode
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.Instant

enum class StockDeductionStatus { PROCESSING, SUCCESS, FAILED }

@Entity
@Table(name = "stock_deduction_history")
class StockDeductionHistory(
    @Id
    @Column(name = "participation_id", length = 36)
    val participationId: String,

    @Column(name = "groupbuy_id", nullable = false, length = 36)
    val groupbuyId: String,

    @Column(name = "product_id", nullable = false, length = 36)
    val productId: String,

    @Column(name = "user_id", nullable = false, length = 36)
    val userId: String,

    @Column(nullable = false)
    val quantity: Int,

    @Column(name = "result_event_id", nullable = false, length = 36)
    val resultEventId: String,

    @Column(name = "created_at", nullable = false, updatable = false)
    val createdAt: Instant = Instant.now()
) {
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    var status: StockDeductionStatus = StockDeductionStatus.PROCESSING
        private set

    @Enumerated(EnumType.STRING)
    @Column(name = "error_code", length = 50)
    var errorCode: ErrorCode? = null
        private set

    @Column(name = "error_message", columnDefinition = "TEXT")
    var errorMessage: String? = null
        private set

    fun succeed() {
        check(status == StockDeductionStatus.PROCESSING)
        status = StockDeductionStatus.SUCCESS
    }

    fun fail(code: ErrorCode) {
        check(status == StockDeductionStatus.PROCESSING)
        errorCode = code
        errorMessage = code.msg
        status = StockDeductionStatus.FAILED
    }

    protected constructor() : this("", "", "", "", 0, "")
}
