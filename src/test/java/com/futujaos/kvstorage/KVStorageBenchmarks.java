package com.futujaos.kvstorage;

import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

/**
 * Benchmarks for {@link com.futujaos.kvstorage.KVStorage}
 */
public class KVStorageBenchmarks extends KVStorageBaseTester {

    final Random random = new Random();

    @Test
    public void timePersistsAndReadsInSameSession() throws IOException {
        final int count = 1000000;
        final int valueLength = 100;

        final Map<Integer, byte[]> keyValues = randomKeyValues(count, valueLength);

        int dummy = 0;

        final long startTimeMillis = System.currentTimeMillis();

        try (KVStorage storage = openTestStorage()) {
            for (Map.Entry<Integer, byte[]> keyValue : keyValues.entrySet()) {
                storage.persist(keyValue.getKey(), keyValue.getValue());
            }

            for (Map.Entry<Integer, byte[]> keyValue : keyValues.entrySet()) {
                final Optional<byte[]> readValue = storage.read(keyValue.getKey());
                if (readValue.isPresent()) {
                    dummy++;
                }
            }
        }

        outputTimeMeasurement("Persists and reads in same session", startTimeMillis);
    }

    @Test
    public void timePersistsAndReadsInNewSession() throws IOException {
        final int count = 1000000;
        final int valueLength = 100;

        final Map<Integer, byte[]> keyValues = randomKeyValues(count, valueLength);

        int dummy = 0;

        final long startTimeMillis = System.currentTimeMillis();

        try (KVStorage storage = openTestStorage()) {
            for (Map.Entry<Integer, byte[]> keyValue : keyValues.entrySet()) {
                storage.persist(keyValue.getKey(), keyValue.getValue());
            }
        }

        try (KVStorage storage = openTestStorage()) {
            for (Map.Entry<Integer, byte[]> keyValue : keyValues.entrySet()) {
                final Optional<byte[]> readValue = storage.read(keyValue.getKey());
                if (readValue.isPresent()) {
                    dummy++;
                }
            }
        }

        outputTimeMeasurement("Persists and reads in new session", startTimeMillis);
    }

    private void outputTimeMeasurement(String operationName, long startTimeMillis) {
        System.out.println(operationName + ": " + (System.currentTimeMillis() - startTimeMillis) + " millis");
    }

    private Map<Integer, byte[]> randomKeyValues(int count, int valuesLength) {
        final Map<Integer, byte[]> keyValues = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            keyValues.put(randomKey(), randomValue(valuesLength));
        }
        return keyValues;
    }

    private int randomKey() {
        return random.nextInt();
    }

    private byte[] randomValue(int valueLength) {
        final byte[] value = new byte[valueLength];
        random.nextBytes(value);
        return value;
    }
}