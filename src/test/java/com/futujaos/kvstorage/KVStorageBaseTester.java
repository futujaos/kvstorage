package com.futujaos.kvstorage;

import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;

public abstract class KVStorageBaseTester {
    File storageFile;
    File metaFile;

    @Before
    public void before() throws IOException {
        storageFile = new File("test.kvs");
        metaFile = new File("test.kvs.meta");
        final boolean storageFileCreated = storageFile.createNewFile();
        final boolean metaFileCreated = metaFile.createNewFile();
        if (!storageFileCreated || !metaFileCreated) {
            throw new IllegalStateException("Test storage file is already exists or does not created");
        }
    }

    @After
    public void after() {
        final boolean storageFileDeleted = storageFile.delete();
        final boolean metaFileCreated = metaFile.delete();
        if (!storageFileDeleted || !metaFileCreated) {
            throw new IllegalStateException("Test storage file does not deleted properly");
        }
    }

    KVStorage openTestStorage() throws IOException {
        final KVStorageConfig config = KVStorageConfig.create(storageFile.getPath(), metaFile.getPath());
        return KVStorageFactory.openStorage(config);
    }
}