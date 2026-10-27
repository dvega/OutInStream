# Out-In Stream

A small Java library that adapts an `OutputStream`-based producer into an `InputStream`.

This is useful when an API writes data to an `OutputStream`, but the consuming code expects an `InputStream`.

## Overview

`OutToInputStream` creates an `InputStream` connected to an internally managed `OutputStream`.

Data written by a producer to the `OutputStream` becomes available to consumers reading from the returned `InputStream`.

It is conceptually similar to Java’s `PipedInputStream` / `PipedOutputStream`, but focuses on clearer error propagation between the output-producing side and the input-consuming side.

## Features

- Convert an `OutputStream` writer into an `InputStream`
- Runs the producer asynchronously
- Supports custom `Executor` usage
- Propagates producer-side exceptions to the reader
- Handles stream closing and interruption scenarios
- Uses Java `Cleaner` to help release blocked producer resources if the input side is abandoned
- Java 11 compatible

## Requirements

- Java 11 or newer
- Gradle wrapper included in the repository

The Gradle build is configured to use a Java 11 toolchain.

## Building

From the project root:
```
bash
./gradlew build
```
On Windows:
```
bash
gradlew.bat build
```
## Running Tests
```
bash
./gradlew test
```
The test suite uses JUnit 4 and covers:

- Normal byte and text streaming
- Empty writes
- Closing behavior
- Producer-side `IOException`
- Producer-side runtime exceptions
- Input/output interruption
- Nested stream conversion
- Base64 wrapping scenarios

## Usage

### Basic Example
```java
import dvega.io.OutToInputStream;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

public class Example {
    public static void main(String[] args) throws Exception {
        InputStream inputStream = OutToInputStream.asInputStream(out -> {
            PrintWriter writer = new PrintWriter(out);

            writer.println("Hello from an OutputStream!");
            writer.println("This can now be read as an InputStream.");

            writer.close();
        });

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            reader.lines().forEach(System.out::println);
        }
    }
}
```
### Using a Custom Executor
```java
import dvega.io.OutToInputStream;

import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CustomExecutorExample {
    public static void main(String[] args) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            InputStream inputStream = OutToInputStream.asInputStream(
                    out -> {
                        out.write("Generated data".getBytes());
                        out.close();
                    },
                    executor,
                    null
            );

            byte[] data = inputStream.readAllBytes();
            System.out.println(new String(data));
        } finally {
            executor.shutdown();
        }
    }
}
```
## Error Handling

If the producer throws an exception while writing to the output side, the input side receives an `OutToInputStream.OutStreamException`.
```java
import dvega.io.OutToInputStream;
import dvega.io.OutToInputStream.OutStreamException;

import java.io.IOException;
import java.io.InputStream;

public class ErrorExample {
    public static void main(String[] args) throws Exception {
        InputStream inputStream = OutToInputStream.asInputStream(out -> {
            out.write("partial data".getBytes());
            throw new IOException("producer failed");
        });

        try {
            inputStream.readAllBytes();
        } catch (OutStreamException e) {
            System.err.println("Producer failed: " + e.getMessage());
        }
    }
}
```
## API

### `OutToInputStream.asInputStream(OutputProducer producer)`

Creates an `InputStream` using the default internal execution and cleanup behavior.
```java
InputStream stream = OutToInputStream.asInputStream(out -> {
    out.write("data".getBytes());
});
```
### `OutToInputStream.asInputStream(OutputProducer producer, Executor executor, Cleaner cleaner)`

Creates an `InputStream` with optional custom execution and cleanup configuration.

- `producer`: writes data to the provided `OutputStream`
- `executor`: runs the producer task; if `null`, a daemon thread is created
- `cleaner`: cleanup mechanism; if `null`, an internal cleaner is used

## Notes

- The producer runs asynchronously.
- Data is transferred from the output side to the input side using internal synchronization.
- Closing the input side notifies the output side.
- Exceptions thrown by the producer are wrapped in `OutStreamException`.
- If the reading thread is interrupted while waiting for data, an `InterruptedIOException` may be thrown.

## License

This project is licensed under the Apache License 2.0.

See [LICENSE](LICENSE) for details.

