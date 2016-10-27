package io.datanerds.newsfeed.consumer.fancy;

@FunctionalInterface
public interface Action<T> {
    void apply(T message);
}
