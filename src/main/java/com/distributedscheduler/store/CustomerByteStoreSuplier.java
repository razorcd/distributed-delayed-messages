package com.distributedscheduler.store;

import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.internal.MongoClientImpl;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

public class CustomerByteStoreSuplier implements KeyValueBytesStoreSupplier {
        private String name;

    public CustomerByteStoreSuplier(String name) {
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public KeyValueStore<Bytes, byte[]> get() {
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:37155");
        return new CustomKeyValueStore(name, mongoClient);
    }

    @Override
    public String metricsScope() {
        return "custom-store";
    }
}