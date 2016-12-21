package com.futujaos.kvstorage.impl;

import com.futujaos.kvstorage.KVStorage;
import com.futujaos.kvstorage.KVStorageConfig;
import com.futujaos.kvstorage.exceptions.KVStorageDamagedException;

import java.io.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

// Storage structure:
// <file_name>.kvs      - file with (key, value) pairs
// <file_name>.kvs.meta - file with storage meta data (such as MD5 of data)
//
// <file_name>.kvs
// | entry bytes | entry bytes | .. | entry bytes |
// | xx xx .. xx | xx xx .. xx | .. | xx xx .. xx |
//
// <file_name>.kvs.meta
// |     MD5     |
// | xx xx .. xx |
//
public class KVStorageImpl implements KVStorage {
    private final File metaFile;
    private final OutputStream os;
    private final InputStream is;
    private final MessageDigest md5Provider;
    private final Map<Integer, Entry> index = new HashMap<>(); // key to entry

    public KVStorageImpl(KVStorageConfig config) throws IOException {
        final File storageFile = new File(config.getStorageFilePath());
        metaFile = new File(config.getMetaFilePath());

        storageFile.createNewFile();
        metaFile.createNewFile();

        is = new BufferedInputStream(new FileInputStream(storageFile.getAbsolutePath()));
        os = new BufferedOutputStream(new FileOutputStream(storageFile.getAbsolutePath(), true));

        try {
            md5Provider = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Runtime does not support MD5 MessageDigest");
        }
        buildIndex();
    }

    @Override
    public Optional<byte[]> read(int key) {
        if (!index.containsKey(key)) {
            return Optional.empty();
        }
        final Entry entry = index.get(key);
        return Optional.of(entry.value);
    }

    @Override
    public void persist(int key, byte[] value) throws IOException {
        final Entry entry = Entry.fromKV(key, value);
        os.write(entry.toByteArray());
        index.put(key, entry);
    }

    @Override
    public void delete(int key) throws IOException {
        final Entry entry = Entry.deleted(key);
        if (!index.containsKey(key)) {
            return;
        }
        os.write(entry.toByteArray());
        index.remove(key);
    }

    @Override
    public void close() throws IOException {
        os.close();
        is.close();

        // Persist storage MD5 in meta file

        index.forEach((key, entry) -> md5Provider.update(entry.toByteArray()));

        final byte[] md5 = md5Provider.digest();
        final FileOutputStream fosMeta = new FileOutputStream(metaFile);
        fosMeta.write(md5);
        fosMeta.close();
    }

    private void buildIndex() throws IOException {
        final FileInputStream fisMeta = new FileInputStream(metaFile);
        final byte[] md5 = new byte[16];
        final int md5ReadResult = fisMeta.read(md5);

        boolean hasData = false;

        while (true) {
            final byte[] countBytes = new byte[4];
            final int countReadResult = is.read(countBytes);
            if (countReadResult < 4) {
                break;
            }

            final int bytesCount = ByteBuffer.wrap(countBytes).getInt();
            final byte[] entryBytesWithoutCount = new byte[bytesCount - 4];

            final int entryReadResult = is.read(entryBytesWithoutCount);
            if (entryReadResult < bytesCount - 4) {
                break;
            }

            final byte[] entryBytes = new byte[bytesCount];
            for (int i = 0; i < entryBytes.length; i++) {
                if (i < 4) {
                    entryBytes[i] = countBytes[i];
                } else {
                    entryBytes[i] = entryBytesWithoutCount[i - 4];
                }
            }

            final Entry entry = Entry.fromBytes(entryBytes);

            if (entry.isDeleted()) {
                // Old entries with same key may exist in storage.
                index.remove(entry.key);
                continue;
            }

            index.put(entry.key, entry);

            hasData = true;
        }

        if (hasData) {
            if (md5ReadResult < 16) {
                throw new IOException("Storage contains data, but meta file does not contain proper MD5.");
            }

            index.forEach((key, entry) -> md5Provider.update(entry.toByteArray()));

            byte[] checkMD5 = md5Provider.digest();

            if (!Arrays.equals(md5, checkMD5)) {
                throw new KVStorageDamagedException();
            }
        }
    }

    // Entry structure:
    //
    // | bytes count | status |     key     |    value    |
    // | xx xx xx xx |   xx   | xx xx xx xx | xx xx .. xx |
    private static final class Entry {
        private static final byte[] EMPTY_VALUE = new byte[0];
        private static final byte STATUS_NORMAL = 0;
        private static final byte STATUS_DELETED = 1;
        private final int bytesCount;
        private final byte status;
        private final int key;
        private final byte[] value;

        public Entry(int bytesCount, byte status, int key, byte[] value) {
            this.bytesCount = bytesCount;
            this.status = status;
            this.key = key;
            this.value = value;
        }

        public boolean isDeleted() {
            return status == STATUS_DELETED;
        }

        public static Entry fromKV(int key, byte[] value) {
            return new Entry(
                    value.length + 4 + 4 + 1,
                    STATUS_NORMAL,
                    key,
                    value
            );
        }

        public static Entry deleted(int key) {
            return new Entry(
                    4 + 4 + 1,
                    STATUS_DELETED,
                    key,
                    EMPTY_VALUE
            );
        }

        public static Entry fromBytes(byte[] bytes) {
            final ByteBuffer bf = ByteBuffer.wrap(bytes);

            final int bytesCount = bf.getInt();
            final int valueLength = bytesCount - (4 + 4 + 1);

            final byte status = bf.get();
            final int key = bf.getInt();
            final byte[] value = new byte[valueLength];
            bf.get(value);

            return new Entry(
                    bytesCount,
                    status,
                    key,
                    value
            );
        }

        public byte[] toByteArray() {
            return ByteBuffer.allocate(bytesCount)
                    .putInt(bytesCount)
                    .put(status)
                    .putInt(key)
                    .put(value)
                    .array();
        }
    }
}