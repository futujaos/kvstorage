# extsort
Simple single-file single-threaded key-value append-only storage. Stores pairs `(long, byte[])`. 
Implements seminal integrity control, calculating MD5 of all storage data, stored in separate 'meta' file.

## Build

```
./gradlew build
```

## Usage

See the [example](src/main/java/com/futujaos/kvstorage/Example.java).
