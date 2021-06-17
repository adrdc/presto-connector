/*
 * Copyright (c) Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.connectors.presto;

import com.facebook.airlift.json.JsonCodec;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.StreamCutImpl;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

@Test
public class StreamCutSupplierTest {
    static JsonCodec<PravegaStreamDescription> jsonCodec = JsonCodec.jsonCodec(PravegaStreamDescription.class);

    static ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void testNoRanges() {
        PravegaStreamDescription desc = streamDesc();
        Assert.assertFalse(desc.getStreamCuts().isPresent());
    }

    @Test
    public void testUnbounded() {
        // both literal string "UNBOUNDED" and empty string "" both mean unbounded
        PravegaStreamDescription desc = streamDesc(new ImmutablePair<>("UNBOUNDED", ""));
        Assert.assertTrue(desc.getStreamCuts().isPresent());
        Assert.assertEquals(desc.getStreamCuts().get().size(), 1);

        StreamCutRange range = desc.getStreamCuts().get().get(0);
        Assert.assertSame(range.getStart(), StreamCut.UNBOUNDED);
        Assert.assertSame(range.getEnd(), StreamCut.UNBOUNDED);
    }

    @Test
    public void testSingle() {
        StreamCut start = new StreamCutBuilder()
                .addOffset(1, 100)
                .build();
        StreamCut end = new StreamCutBuilder()
                .addOffset(1, 200)
                .build();
        PravegaStreamDescription desc = streamDesc(
                new ImmutablePair<>(start.asText(), end.asText())
        );
        Assert.assertTrue(desc.getStreamCuts().isPresent());
        Assert.assertEquals(desc.getStreamCuts().get().size(), 1);

        StreamCutRange range = desc.getStreamCuts().get().get(0);
        Assert.assertEquals(range.getStart(), start);
        Assert.assertEquals(range.getEnd(), end);
    }

    @Test
    public void testStaticSupplierSingle() {
        PravegaStreamDescription desc = streamDesc(new ImmutablePair<>("UNBOUNDED", ""));
        StreamCutSupplier supplier = new StreamCutSupplier(null, tableHandle(desc), null);

        StreamCutRange range = supplier.get();
        Assert.assertSame(range.getStart(), StreamCut.UNBOUNDED);
        Assert.assertSame(range.getEnd(), StreamCut.UNBOUNDED);

        Assert.assertNull(supplier.get()); // only 1 range, no more
    }

    @Test
    public void testStaticSupplierMulti() {
        // 2 cuts:
        // head -> 200
        // 500 -> tail
        StreamCut end1 = new StreamCutBuilder()
                .addOffset(1, 200)
                .build();
        StreamCut start2 = new StreamCutBuilder()
                .addOffset(1, 500)
                .build();
        PravegaStreamDescription desc = streamDesc(
                new ImmutablePair<>("UNBOUNDED", end1.asText()),
                new ImmutablePair<>(start2.asText(), "UNBOUNDED")
        );
        StreamCutSupplier supplier = new StreamCutSupplier(null, tableHandle(desc), null);

        StreamCutRange range = supplier.get();
        Assert.assertSame(range.getStart(), StreamCut.UNBOUNDED);
        Assert.assertEquals(range.getEnd(), end1);

        range = supplier.get();
        Assert.assertEquals(range.getStart(), start2);
        Assert.assertSame(range.getEnd(), StreamCut.UNBOUNDED);

        Assert.assertNull(supplier.get()); // 2 ranges, no more
    }

    PravegaStreamDescription streamDesc(Pair<String, String>... sc) {
        Map<String, Object> root = new HashMap<>();

        root.put("schemaName", "schema");
        root.put("tableName", "table");
        root.put("objectName", "table");

        for (Pair<String, String> p : sc) {
            List<Object> list = root.containsKey("streamCuts")
                    ? (List<Object>) root.get("streamCuts")
                    : null;
            if (list == null) {
                list = new ArrayList<>();
                root.put("streamCuts", list);
            }

            Map<String, String> map = new HashMap<>();
            map.put("base64Start", p.getLeft());
            map.put("base64End", p.getRight());
            list.add(map);
        }

        try {
            String tmp = objectMapper.writeValueAsString(root);
            System.out.println(tmp);
            return jsonCodec.fromJson(objectMapper.writeValueAsString(root));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    class StreamCutBuilder
    {
        Map<Segment, Long> positions = new HashMap<>();

        StreamCutBuilder addOffset(long id, long offset)
        {
            positions.put(new Segment("scope", "stream", id), offset);
            return this;
        }

        StreamCut build()
        {
            return new StreamCutImpl(Stream.of("scope", "stream"), positions);
        }
    }

    PravegaTableHandle tableHandle(PravegaStreamDescription streamDesc)
    {
        // only stream cuts will be used from this handle
        return new PravegaTableHandle("pravega",
                streamDesc.getSchemaName().get(),
                streamDesc.getTableName(),
                streamDesc.getObjectName(),
                ObjectType.STREAM,
                Optional.empty(),
                streamDesc.getStreamCuts(),
                new ArrayList<>(),
                "");
    }
}
