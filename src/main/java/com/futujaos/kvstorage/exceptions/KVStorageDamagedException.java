package com.futujaos.kvstorage.exceptions;

import java.io.IOException;

/**
 * Throws when calculated and stored MD5 hashed are differ.
 * <p>
 * This means, most likely, corruption of storage or meta file.
 */
public class KVStorageDamagedException extends IOException {
    public KVStorageDamagedException() {
    }
}