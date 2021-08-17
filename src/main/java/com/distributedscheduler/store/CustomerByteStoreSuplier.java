package com.distributedscheduler.store;

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
        return new CustomKeyValueStore(name);
    }

    @Override
    public String metricsScope() {
        return "custom-store";
    }
}