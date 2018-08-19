Building OHC from source
========================

OHC since 0.7.0 can be built from source using Java 8 or Java 11.
It is recommended to build with Java 11 to include support for Java 11 features in a multi-release jar.

```
mvn clean install -DskipJmh
```

`-DskipJmh` skips running the microbenchmarks

Unit tests are run against the Java 8 only classes first. Another run will include the Java 11 classes.
