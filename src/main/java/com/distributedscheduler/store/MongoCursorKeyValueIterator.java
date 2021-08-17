package com.distributedscheduler.store;

import com.mongodb.client.MongoCursor;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.bson.Document;

public class MongoCursorKeyValueIterator implements KeyValueIterator<Bytes, byte[]> {
    private final MongoCursor<Document> mongoCursor;

    public MongoCursorKeyValueIterator(final MongoCursor<Document> mongoCursor) {
        this.mongoCursor = mongoCursor;
    }

    @Override
    public boolean hasNext() {
        return mongoCursor.hasNext();
    }

    @Override
    public KeyValue<Bytes, byte[]> next() {
        Document nextDocument = mongoCursor.next();
        return new KeyValue<>(Bytes.wrap(nextDocument.get("_id", String.class).getBytes()), String.valueOf(nextDocument.get("value")).getBytes());
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