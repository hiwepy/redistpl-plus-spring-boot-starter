package org.springframework.data.redis.core;

import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ErrorHandler;

import java.util.List;

/**
 * NestedErrorHandler.
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
@Slf4j
public class NestedErrorHandler implements ErrorHandler {

    List<ErrorHandler> errorHandlers;

    public NestedErrorHandler(List<ErrorHandler> errorHandlers){
        this.errorHandlers = errorHandlers;
    }

    @Override
    public void handleError(Throwable e) {
        if(CollectionUtils.isEmpty(errorHandlers)){
            log.error("Stream Message handle Error :", e);
        } else {
            for (ErrorHandler handler : errorHandlers ) {
                try {
                    handler.handleError(e);
                } catch (Exception ex){
                    log.error("Stream Message handle Error :", e);
                }
            }
        }
    }
}
