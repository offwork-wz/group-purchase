package inventory_service.service

import inventory_service.domain.Inventory
import inventory_service.domain.StockDeductionHistory
import inventory_service.domain.StockDeductionStatus
import inventory_service.global.error.BusinessException
import inventory_service.global.error.ErrorCode
import inventory_service.repository.InventoryRepository
import inventory_service.repository.StockRestoreHistoryRepository
import inventory_service.repository.StockDeductionHistoryRepository
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.util.UUID

@Service
class InventoryService(
    private val inventoryRepository: InventoryRepository,
    private val stockRestoreHistoryRepository: StockRestoreHistoryRepository,
    private val stockDeductionHistoryRepository: StockDeductionHistoryRepository
) {
    private val log = KotlinLogging.logger {}

    @Transactional
    fun deductStockForParticipation(
        participationId: String, groupbuyId: String, productId: String, userId: String, quantity: Int
    ): StockDeductionHistory {
        require(participationId.isNotBlank()) { "participationId는 비어 있을 수 없습니다." }
        val insertedRows = stockDeductionHistoryRepository.insertIgnoringDuplicate(
            participationId, groupbuyId, productId, userId, quantity, UUID.randomUUID().toString()
        )
        val history = stockDeductionHistoryRepository.findById(participationId)
            .orElseThrow { IllegalStateException("재고 차감 이력을 찾을 수 없음. participationId=$participationId") }

        if (insertedRows == 0) {
            check(history.status != StockDeductionStatus.PROCESSING) {
                "미완료 재고 차감 이력. participationId=$participationId"
            }
            if (history.groupbuyId != groupbuyId || history.productId != productId ||
                history.userId != userId || history.quantity != quantity
            ) {
                log.warn {
                    "참여 재고 차감 중복 내용 불일치. participationId=$participationId, " +
                        "storedGroupbuyId=${history.groupbuyId}, receivedGroupbuyId=$groupbuyId, " +
                        "storedProductId=${history.productId}, receivedProductId=$productId, " +
                        "storedUserId=${history.userId}, receivedUserId=$userId, " +
                        "storedQuantity=${history.quantity}, receivedQuantity=$quantity"
                }
            } else {
                log.info { "이미 처리한 재고 차감 결과 재사용. participationId=$participationId, status=${history.status}" }
            }
            return history
        }

        if (quantity <= 0) {
            history.fail(ErrorCode.INVALID_QUANTITY)
            return history
        }

        val inventory = inventoryRepository.findByProductIdWithLock(productId)
            .orElseThrow { BusinessException(ErrorCode.PRODUCT_NOT_FOUND) }
        if (inventory.stockQuantity < quantity) {
            history.fail(ErrorCode.INSUFFICIENT_STOCK)
        } else {
            inventory.decrease(quantity)
            history.succeed()
        }
        return history
    }

    @Transactional
    fun restoreStockForOrder(orderId: String, productId: String, quantity: Int) {
        if (quantity <= 0) {
            throw BusinessException(ErrorCode.INVALID_QUANTITY)
        }

        val insertedRows = stockRestoreHistoryRepository.insertIgnoringDuplicate(orderId, productId, quantity)
        if (insertedRows == 0) {
            val history = stockRestoreHistoryRepository.findById(orderId)
                .orElseThrow { IllegalStateException("재고 복구 이력을 찾을 수 없음. orderId=$orderId") }
            if (history.productId != productId || history.quantity != quantity) {
                log.warn {
                    "주문 취소 재고 복구 중복 내용 불일치. orderId=$orderId, " +
                        "storedProductId=${history.productId}, storedQuantity=${history.quantity}, " +
                        "receivedProductId=$productId, receivedQuantity=$quantity"
                }
            } else {
                log.info { "이미 재고를 복구한 주문. orderId=$orderId" }
            }
            return
        }

        val inventory = inventoryRepository.findByProductIdWithLock(productId)
            .orElseThrow { BusinessException(ErrorCode.PRODUCT_NOT_FOUND) }
        inventory.increase(quantity)
    }

    @Transactional
    fun decreaseStock(productId: String, quantity: Int) {
        val inventory = inventoryRepository.findByProductIdWithLock(productId)
            .orElseThrow { BusinessException(ErrorCode.PRODUCT_NOT_FOUND) }

        inventory.decrease(quantity)
        // JPA dirty checking will handle the update
    }

    @Transactional
    fun increaseStock(productId: String, quantity: Int) {
        val inventory = inventoryRepository.findByProductIdWithLock(productId)
            .orElseThrow { BusinessException(ErrorCode.PRODUCT_NOT_FOUND) }

        inventory.increase(quantity)
        // JPA dirty checking will handle the update
    }

    @Transactional
    fun registStock(productId: String, quantity: Int) {
        if (inventoryRepository.existsById(productId)) {
            throw BusinessException(ErrorCode.STOCK_ALREADY_EXIST)
        }
        inventoryRepository.save(Inventory(productId, quantity))

    }


}
