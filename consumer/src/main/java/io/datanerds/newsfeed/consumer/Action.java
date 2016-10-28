package io.datanerds.newsfeed.consumer;

@FunctionalInterface
public interface Action<T> {
    void apply(T message);
}
