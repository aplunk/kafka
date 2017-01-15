/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.jmh;

import kafka.message.ByteBufferMessageSet;
import kafka.message.CompressionCodec;
import kafka.message.CompressionCodec$;
import kafka.message.Message;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class CompressionBenchmark {
    private final static CompressionCodec NO_COMPRESSION_CODEC
        = CompressionCodec$.MODULE$.getCompressionCodec(0);
    private final static CompressionCodec GZIP_COMPRESSION_CODEC
        = CompressionCodec$.MODULE$.getCompressionCodec(1);
    private final static CompressionCodec SNAPPY_COMPRESSION_CODEC
        = CompressionCodec$.MODULE$.getCompressionCodec(2);
    private final static CompressionCodec LZ4_COMPRESSION_CODEC
        = CompressionCodec$.MODULE$.getCompressionCodec(3);
    private final static CompressionCodec ZSTD_COMPRESSION_CODEC
        = CompressionCodec$.MODULE$.getCompressionCodec(4);

    private Seq<Message> messageSeq;

    @Setup(Level.Trial)
    public void setup() {
        byte[] payload1 = new byte[1000];
        byte[] payload2 = new byte[1000];
        byte[] payload3 = new byte[1000];
        for (int i = 0; i < 1000; ++i) {
            payload1[i] = (byte) i;
            payload2[i] = (byte) (1000 + i);
            payload3[i] = (byte) (3000 + i);
        }
        List<Message> messages = new ArrayList<>();
        messages.add(new Message(payload1, Message.NoTimestamp(), Message.MagicValue_V1()));
        messages.add(new Message(payload2, Message.NoTimestamp(), Message.MagicValue_V1()));
        messages.add(new Message(payload3, Message.NoTimestamp(), Message.MagicValue_V1()));
        messageSeq = JavaConversions.asScalaBuffer(messages).toSeq();

        System.out.println();
        ByteBufferMessageSet uncompressed = new ByteBufferMessageSet(NO_COMPRESSION_CODEC, messageSeq);
        System.out.println(String.format("Uncompressed:\t%d", uncompressed.sizeInBytes()));
        ByteBufferMessageSet gzip = new ByteBufferMessageSet(GZIP_COMPRESSION_CODEC, messageSeq);
        System.out.println(String.format("Gzip:\t%d", gzip.sizeInBytes()));
        ByteBufferMessageSet snappy = new ByteBufferMessageSet(SNAPPY_COMPRESSION_CODEC, messageSeq);
        System.out.println(String.format("Snappy:\t%d", snappy.sizeInBytes()));
        ByteBufferMessageSet lz4 = new ByteBufferMessageSet(LZ4_COMPRESSION_CODEC, messageSeq);
        System.out.println(String.format("LZ4:\t%d", lz4.sizeInBytes()));
        ByteBufferMessageSet zstd = new ByteBufferMessageSet(ZSTD_COMPRESSION_CODEC, messageSeq);
        System.out.println(String.format("ZStandard:\t%d", zstd.sizeInBytes()));
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void testGzip(Blackhole blackHole) {
        ByteBufferMessageSet gzip = new ByteBufferMessageSet(GZIP_COMPRESSION_CODEC, messageSeq);
        blackHole.consume(gzip);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void testSnappy(Blackhole blackHole) {
        ByteBufferMessageSet snappy = new ByteBufferMessageSet(SNAPPY_COMPRESSION_CODEC, messageSeq);
        blackHole.consume(snappy);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void testLZ4(Blackhole blackHole) {
        ByteBufferMessageSet lz4 = new ByteBufferMessageSet(LZ4_COMPRESSION_CODEC, messageSeq);
        blackHole.consume(lz4);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void testZStandard(Blackhole blackHole) {
        ByteBufferMessageSet zstd = new ByteBufferMessageSet(ZSTD_COMPRESSION_CODEC, messageSeq);
        blackHole.consume(zstd);
    }
}
