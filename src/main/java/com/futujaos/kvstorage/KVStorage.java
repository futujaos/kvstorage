package com.futujaos.kvstorage;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

/**
 * Key-value storage, persists data into single file.
 * <p>
 * Keys are 'int' values, values are 'byte[]' arrays.
 * <p>
 * Supports only single-threaded access.
 */
public interface KVStorage extends Closeable {

    Optional<byte[]> read(int key);

    void persist(int key, byte[] value) throws IOException;

    void delete(int key) throws IOException;
}