package com.distributedscheduler.store;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Iterator;
import java.util.List;

public class InMemoryKeyValueIterator implements KeyValueIterator<Bytes, byte[]> {
    private final Iterator<KeyValue<Bytes, byte[]>> iter;

    private InMemoryKeyValueIterator(final List<KeyValue<Bytes, byte[]>> list) {
        this.iter = list.iterator();
    }

    @Override
    public boolean hasNext() {
        return iter.hasNext();
    }

    @Override
    public KeyValue<Bytes, byte[]> next() {
        return iter.next();
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public Bytes peekNextKey() {
        throw new UnsupportedOperationException("peekNextKey() not supported in " + getClass().getName());
    }
}