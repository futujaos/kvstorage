package com.futujaos.kvstorage;

import com.futujaos.kvstorage.exceptions.KVStorageDamagedException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Optional;

import static org.junit.Assert.*;

/**
 * Tests for {@link com.futujaos.kvstorage.KVStorage}
 */
public class KVStorageTests extends KVStorageBaseTester {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testEmpty() throws IOException {
        try (KVStorage storage = openTestStorage()) {
            final Optional<byte[]> value = storage.read(42);

            assertFalse("Storage should be empty", value.isPresent());
        }
    }

    @Test
    public void testReadInSameSession() throws IOException {
        final int key1 = 7;
        final int key2 = -42;
        final int key3 = 200;

        final byte[] value1 = new byte[]{1, -50, 2};
        final byte[] value2 = new byte[]{66, -8, 13, 0, 20};
        final byte[] value3 = new byte[]{-100, 7, 3, 126};

        try (KVStorage storage = openTestStorage()) {
            storage.persist(key1, value1);
            storage.persist(key2, value2);
            storage.persist(key3, value3);

            final Optional<byte[]> value = storage.read(key2);

            assertTrue("Value should exist", value.isPresent());
            assertArrayEquals("Value should equals to original", value2, value.get());
        }
    }

    @Test
    public void testReadInNewSession() throws IOException {
        final int key1 = 7;
        final int key2 = -42;
        final int key3 = 200;

        final byte[] value1 = new byte[]{1, -50, 2};
        final byte[] value2 = new byte[]{66, -8, 13, 0, 20};
        final byte[] value3 = new byte[]{-100, 7, 3, 126};

        try (KVStorage storage = openTestStorage()) {
            storage.persist(key1, value1);
            storage.persist(key2, value2);
            storage.persist(key3, value3);
        }

        try (KVStorage storage = openTestStorage()) {
            final Optional<byte[]> value = storage.read(key2);

            assertTrue("Value should exist", value.isPresent());
            assertArrayEquals("Value should equals to original", value2, value.get());
        }
    }

    @Test
    public void testReadLaterPersistInSameSession() throws IOException {
        final int key = 50;
        final byte[] oldValue = new byte[]{67, -100, 1, 3};
        final byte[] newValue = new byte[]{7, -5};

        try (KVStorage storage = openTestStorage()) {
            storage.persist(key, oldValue);
            storage.persist(key, newValue);

            final Optional<byte[]> value = storage.read(key);

            assertTrue("Value should exist", value.isPresent());
            assertArrayEquals("Value should equal to later value", newValue, value.get());
        }
    }

    @Test
    public void testReadLaterPersistInNewSession() throws IOException {
        final int key = 50;
        final byte[] oldValue = new byte[]{67, -100, 1, 3};
        final byte[] newValue = new byte[]{7, -5};

        try (KVStorage storage = openTestStorage()) {
            storage.persist(key, oldValue);
            storage.persist(key, newValue);
        }

        try (KVStorage storage = openTestStorage()) {
            final Optional<byte[]> value = storage.read(key);

            assertTrue("Value should exist", value.isPresent());
            assertArrayEquals("Value should equal to later value", newValue, value.get());
        }
    }

    @Test
    public void testReadByOtherKeyInSameSession() throws IOException {
        final int key = 7;
        final int otherKey = 8;
        final byte[] value = new byte[]{7, -2};

        try (KVStorage storage = openTestStorage()) {
            storage.persist(key, value);

            Optional<byte[]> otherValue = storage.read(otherKey);

            assertFalse("Value should not exist by other key", otherValue.isPresent());
        }
    }

    @Test
    public void testReadByOtherKeyInNewSession() throws IOException {
        final int key = 7;
        final int otherKey = 8;
        final byte[] value = new byte[]{7, -2};

        try (KVStorage storage = openTestStorage()) {
            storage.persist(key, value);
        }

        try (KVStorage storage = openTestStorage()) {
            Optional<byte[]> otherValue = storage.read(otherKey);

            assertFalse("Value should not exist by other key", otherValue.isPresent());
        }
    }

    @Test
    public void testStorageDamageDetection() throws IOException {
        final int key = 42;
        final byte[] value = new byte[]{7, -80, 6, 3};
        final byte[] noise = new byte[]{22, -90};

        try (KVStorage storage = openTestStorage()) {
            storage.persist(key, value);
        }

        RandomAccessFile file = new RandomAccessFile(storageFile, "rw");
        file.seek(9);
        file.write(noise);
        file.close();

        exception.expect(KVStorageDamagedException.class);

        openTestStorage();
    }

    @Test
    public void testReadDeletedEntryInSameSession() throws IOException {
        final int key = 5;
        final byte[] value = {3, -9, 2};

        try (KVStorage storage = openTestStorage()) {
            storage.persist(key, value);

            storage.delete(key);

            final Optional<byte[]> deletedValue = storage.read(5);

            assertFalse("Value should not exist, because it was deleted", deletedValue.isPresent());
        }
    }

    @Test
    public void testReadDeletedEntryInNewSession() throws IOException {
        final int key = 5;
        final byte[] value = {3, -9, 2};

        try (KVStorage storage = openTestStorage()) {
            storage.persist(key, value);
        }

        try (KVStorage storage = openTestStorage()) {
            storage.delete(key);
        }

        try (KVStorage storage = openTestStorage()) {
            final Optional<byte[]> deletedValue = storage.read(5);

            assertFalse("Value should not exist, because it was deleted", deletedValue.isPresent());
        }
    }
}