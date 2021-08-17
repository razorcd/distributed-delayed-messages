package com.distributedscheduler.store;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.NoSuchElementException;

public class EmptyKeyValueIterator<K, V> implements KeyValueIterator<K, V> {

    @Override
    public void close() {
    }

    @Override
    public K peekNextKey() {
        throw new NoSuchElementException();
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public KeyValue<K, V> next() {
        throw new NoSuchElementException();
    }

}