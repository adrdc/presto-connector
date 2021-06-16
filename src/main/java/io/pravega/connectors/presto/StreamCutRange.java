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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.pravega.client.stream.StreamCut;

import java.io.Serializable;

import static com.google.common.base.MoreObjects.toStringHelper;

public class StreamCutRange
        implements Serializable
{
    public static final StreamCutRange NULL_PAIR = new StreamCutRange((StreamCut) null, null);

    private final StreamCut start;

    private final StreamCut end;

    public StreamCutRange(StreamCut start, StreamCut end)
    {
        this.start = start;
        this.end = end;
    }

    @JsonCreator
    public StreamCutRange(@JsonProperty("base64Start") String base64Start,
                          @JsonProperty("base64End") String base64End)
    {
        this.start = base64Start == null || base64Start.isEmpty()
                ? StreamCut.UNBOUNDED
                : StreamCut.from(base64Start);
        this.end = base64End == null || base64End.isEmpty()
                ? StreamCut.UNBOUNDED
                : StreamCut.from(base64End);
    }

    @JsonProperty
    public String getBase64Start()
    {
        return start.asText();
    }

    @JsonProperty
    public String getBase64End()
    {
        return end.asText();
    }

    public StreamCut getStart()
    {
        return start;
    }

    public StreamCut getEnd()
    {
        return end;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("start", start == null ? "(null)" : start.asText())
                .add("startPositions",
                        start == null || start.asImpl() == null
                                ? "(null)"
                                : start.asImpl().getPositions())
                .add("end", end == null ? "(null)" : end.asText())
                .add("endPositions",
                        end == null || end.asImpl() == null
                                ? "(null)"
                                : end.asImpl().getPositions())
                .toString();
    }
}
