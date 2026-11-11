package groupbuy_service.participation.domain;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

public record ParticipationRequest(
    @NotBlank
    String productId,
    @NotBlank
    String userId,
    @Positive
    int quantity
) {
}
