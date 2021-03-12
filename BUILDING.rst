Building OHC from source
========================

OHC since 0.7.1 can be built from source using Java 11 or newer (tested with Java 11 + 15).
OHC runs against Java 8 and newer.

```
mvn clean install -DskipJmh
```

`-DskipJmh` skips running the microbenchmarks

Unit tests are run against the Java 8 only classes first. Another run will include the Java 11 classes.
