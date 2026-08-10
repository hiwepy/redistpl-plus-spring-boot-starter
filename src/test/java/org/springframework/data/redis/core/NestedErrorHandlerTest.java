package org.springframework.data.redis.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import java.util.*;

import org.junit.jupiter.api.Test;
import org.springframework.util.ErrorHandler;

/**
 * Tests for {@link NestedErrorHandler}.
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 */
class NestedErrorHandlerTest {

    @Test
    void handleErrorWithEmptyHandlers() {
        NestedErrorHandler handler = new NestedErrorHandler(Collections.emptyList());
        handler.handleError(new RuntimeException("test"));
        // Should not throw
    }

    @Test
    void handleErrorWithNullHandlers() {
        NestedErrorHandler handler = new NestedErrorHandler(null);
        handler.handleError(new RuntimeException("test"));
        // Should not throw
    }

    @Test
    void handleErrorWithDelegates() {
        ErrorHandler delegate1 = mock(ErrorHandler.class);
        ErrorHandler delegate2 = mock(ErrorHandler.class);
        List<ErrorHandler> delegates = Arrays.asList(delegate1, delegate2);

        NestedErrorHandler handler = new NestedErrorHandler(delegates);
        RuntimeException error = new RuntimeException("test");
        handler.handleError(error);

        verify(delegate1).handleError(error);
        verify(delegate2).handleError(error);
    }

    @Test
    void handleErrorWithDelegateThrowingException() {
        ErrorHandler delegate1 = mock(ErrorHandler.class);
        ErrorHandler delegate2 = mock(ErrorHandler.class);
        doThrow(new RuntimeException("delegate error")).when(delegate1).handleError(any());

        List<ErrorHandler> delegates = Arrays.asList(delegate1, delegate2);

        NestedErrorHandler handler = new NestedErrorHandler(delegates);
        RuntimeException error = new RuntimeException("test");
        handler.handleError(error);

        verify(delegate1).handleError(error);
        verify(delegate2).handleError(error);
    }
}
