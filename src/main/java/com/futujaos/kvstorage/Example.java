package com.futujaos.kvstorage;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

public class Example {

    public static void main(String[] args) {
        System.out.println("KVStorage checker\n");

        int key1 = 6;
        byte[] value1 = {1, -7, 42, 3, 8, 1};

        int key2 = -800;
        byte[] value2 = {9};

        int key3 = 777;
        byte[] value3 = {-50, 43, 1, 0, 0, 62, 8};

        try (final KVStorage storage = KVStorageFactory.openStorage()) {

            storage.persist(key1, value1);
            storage.persist(key2, value2);
            storage.persist(key3, value3);

        } catch (IOException e) {
            e.printStackTrace();
        }

        try (final KVStorage storage = KVStorageFactory.openStorage()) {

            Optional<byte[]> readValue2 = storage.read(key2);
            System.out.println(Arrays.toString(readValue2.get()));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}