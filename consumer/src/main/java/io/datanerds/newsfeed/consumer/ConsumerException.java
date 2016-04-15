package io.datanerds.newsfeed.consumer;

public class ConsumerException extends RuntimeException {

    public ConsumerException(String message, Throwable ex) {
        super(message, ex);
    }
}
