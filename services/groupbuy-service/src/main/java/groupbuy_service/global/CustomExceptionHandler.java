package groupbuy_service.global;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@Slf4j
@RestControllerAdvice
public class CustomExceptionHandler {

    @ExceptionHandler({MethodArgumentNotValidException.class, HttpMessageNotReadableException.class})
    ResponseEntity<ErrorResponse> handleInvalidRequest(Exception e) {
        log.warn("Invalid request: {}", e.getMessage());
        ErrorResponse response = ErrorResponse.from(ErrorCode.INVALID_REQUEST);
        return ResponseEntity.badRequest().body(response);
    }

    @ExceptionHandler(BusinessException.class)
    ResponseEntity<ErrorResponse> handleBusinessException(BusinessException e) {
        log.warn("BusinessException: {}", e.getMessage());
        ErrorResponse response = ErrorResponse.from(e.getErrorCode());

        return ResponseEntity.status(response.getStatus()).body(response);
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<ErrorResponse> handleException(Exception e) {
        log.error("Exception: ", e);
        ErrorResponse response = ErrorResponse.from(ErrorCode.INTERNAL_ERROR);

        return ResponseEntity.status(response.getStatus()).body(response);
    }

}
