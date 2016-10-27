package io.datanerds.newsfeed.consumer.simple;

@FunctionalInterface
public interface ExceptionHandler {
    void handle(Exception ex);
}
