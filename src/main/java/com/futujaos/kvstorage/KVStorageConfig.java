package com.futujaos.kvstorage;

/**
 * Configuration for storage.
 */
public class KVStorageConfig {
    private static final KVStorageConfig DEFAULT_CONFIG = new KVStorageConfig(
            "storage.kvs",
            "storage.kvs.meta"
    );
    private final String storageFilePath;
    private final String metaFilePath;

    /**
     * Created new config with specified params.
     *
     * @param storageFilePath Path to storage file.
     * @param metaFilePath    Path to meta file.
     * @return Config.
     */
    public static KVStorageConfig create(String storageFilePath, String metaFilePath) {
        return new KVStorageConfig(storageFilePath, metaFilePath);
    }

    /**
     * Returns default config.
     * <p>
     * For default config, storage and meta files are in application working directory.
     * <p>
     * Default name for storage file: "storage.kvs".
     * <p>
     * Default name for meta file:    "storage.kvs.meta".
     *
     * @return Default config.
     */
    public static KVStorageConfig getDefaultConfig() {
        return DEFAULT_CONFIG;
    }

    private KVStorageConfig(String storageFilePath, String metaFilePath) {
        this.storageFilePath = storageFilePath;
        this.metaFilePath = metaFilePath;
    }

    public String getStorageFilePath() {
        return storageFilePath;
    }

    public String getMetaFilePath() {
        return metaFilePath;
    }
}