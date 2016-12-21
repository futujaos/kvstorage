package com.futujaos.kvstorage;

import com.futujaos.kvstorage.impl.KVStorageImpl;

import java.io.IOException;

/**
 * Factory for creating {@link com.futujaos.kvstorage.KVStorage} instances.
 */
public class KVStorageFactory {
    /**
     * Opens storage with specified config.
     *
     * @param config Storage config.
     * @return Storage instance.
     * @throws IOException
     */
    public static KVStorage openStorage(KVStorageConfig config) throws IOException {
        return new KVStorageImpl(config);
    }

    /**
     * Opens storage with default config.
     * <p>
     * For details about default config see {@link com.futujaos.kvstorage.KVStorageConfig}.
     *
     * @return Storage instance.
     * @throws IOException
     */
    public static KVStorage openStorage() throws IOException {
        return new KVStorageImpl(KVStorageConfig.getDefaultConfig());
    }
}