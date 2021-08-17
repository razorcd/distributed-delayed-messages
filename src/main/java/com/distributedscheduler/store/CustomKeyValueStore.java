package com.distributedscheduler.store;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.DelegatingPeekingKeyValueIterator;
import org.bson.Document;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

public class CustomKeyValueStore implements KeyValueStore<Bytes, byte[]> {

    private final String name;
    private final MongoClient mongoClient;
    private final MongoDatabase database;
    private MongoCollection<Document> databaseCollection;


    private volatile boolean open = false;
    private long size = 0L; // SkipListMap#size is O(N) so we just do our best to track it

    public CustomKeyValueStore(final String name, final MongoClient mongoClient) {
        this.name = name;
        this.mongoClient = mongoClient;
        this.database = mongoClient.getDatabase(name);
    }

    @Override
    public String name() {
        return name;
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        TaskId taskId = context.taskId();
        String collectionName = String.valueOf(taskId.partition);
        database.createCollection(collectionName);
        databaseCollection = database.getCollection(collectionName);

        size = 0;
        if (root != null) {
            // register the store
            context.register(root, (key, value) -> put(Bytes.wrap(key), value));
        }

        open = true;
    }

//    public void init(final StateStoreContext context, final StateStore root) {
//        context.taskId()
//    }

    @Override
    public boolean persistent() {
        return true;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public synchronized byte[] get(final Bytes key) {
        Document document = databaseCollection.find(new Document("_id", key.toString())).first();
        return document==null ? null : String.valueOf(document.get("value")).getBytes();
    }

    @Override
    public synchronized void put(final Bytes key, final byte[] value) {
        if (value == null) {
            DeleteResult deleteResult = databaseCollection.deleteOne(new Document("_id", key.toString()));
            size -= deleteResult.getDeletedCount();
        } else {
            String stringValue = new String(value, StandardCharsets.UTF_8);
            databaseCollection.insertOne(new Document("_id", key.toString()).append("value", stringValue));
            size ++;
        }
    }

    @Override
    public synchronized byte[] putIfAbsent(final Bytes key, final byte[] value) {
        final byte[] originalValue = get(key);
        if (originalValue == null) {
            put(key, value);
        }
        return originalValue;
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        List<Document> documentList = entries.stream()
                .map(kv -> new Document("_id", kv.key.toString()).append("value", new String(kv.value, StandardCharsets.UTF_8)))
                .collect(Collectors.toList());
        databaseCollection.insertMany(documentList);
        size += documentList.size();
    }

//    @Override
//    public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefixScan(final P prefix, final PS prefixKeySerializer) {
//        Objects.requireNonNull(prefix, "prefix cannot be null");
//        Objects.requireNonNull(prefixKeySerializer, "prefixKeySerializer cannot be null");
//
//        final Bytes from = Bytes.wrap(prefixKeySerializer.serialize(null, prefix));
//        final Bytes to = Bytes.increment(from);
//
//        return new DelegatingPeekingKeyValueIterator<>(
//                name,
//                new InMemoryKeyValueIterator(map.subMap(from, true, to, false).keySet(), true)
//        );
//    }

    @Override
    public synchronized byte[] delete(final Bytes key) {
        Document deletedDocument = databaseCollection.findOneAndDelete(new Document("_id", key.toString()));
        size -= deletedDocument == null ? 0 : 1;
        return String.valueOf(deletedDocument.get("_id")).getBytes();
    }

    @Override
    public synchronized KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
        return range(from, to, true);
    }

    @Override
    public synchronized KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from, final Bytes to) {
        return range(from, to, false);
    }

    private KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to, final boolean forward) {

        throw new UnsupportedOperationException("Range not implemented.");
//
//        if (from.compareTo(to) > 0) {
//            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. " +
//                    "This may be due to range arguments set in the wrong order, " +
//                    "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
//                    "Note that the built-in numerical serdes do not follow this for negative numbers");
//            return new EmptyKeyValueIterator<>();
//        }
//
//        return new DelegatingPeekingKeyValueIterator<>(
//                name,
////                new InMemoryKeyValueIterator(map.subMap(from, true, to, true).keySet(), forward));
//                new EmptyKeyValueIterator<>());
    }

    @Override
    public synchronized KeyValueIterator<Bytes, byte[]> all() {
        MongoCursor<Document> mongoCursor = databaseCollection.find().batchSize(1000).iterator();

        return new DelegatingPeekingKeyValueIterator<>(name, new MongoCursorKeyValueIterator(mongoCursor));
    }

    @Override
    public synchronized KeyValueIterator<Bytes, byte[]> reverseAll() {
        throw new UnsupportedOperationException("ReverseAll not implemented.");
    }

    @Override
    public long approximateNumEntries() {
        return size;
    }

    @Override
    public void flush() {
        // no need
    }

    @Override
    public void close() {
        mongoClient.close();
        size = 0;
        open = false;
    }
}
