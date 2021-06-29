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
import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.pravega.connectors.presto.schemamanagement.CompositeSchemaRegistry;

import javax.inject.Inject;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.pravega.connectors.presto.util.PravegaNameUtils.multiSourceStream;
import static java.util.Objects.requireNonNull;

// pravega scope is a namespace for streams.  stream is unique within scope.
// presto schema is like a database, with collection of tables.
// we will map scope->schema and stream->table
// scope will be our database and streams will be tables in the database
//
// a stream schema could be "local". local definitions take precedence.
// .json file in known directory with naming format <schema>.<table>.json
// there could be any number of local schemas.
//
// additionally a local stream schema could be a composite table.  this is called 'multi-source'.
// stream name will be a regex.  it can match 1 or more source streams.  when this single table is
// queried we will consider events from all source streams
public class PravegaTableDescriptionSupplier {
    private static final Logger log = Logger.get(PravegaTableDescriptionSupplier.class);

    private final CompositeSchemaRegistry schemaRegistry;

    private final Cache<String, List<PravegaTableName>> schemaCache;

    private final LoadingCache<PravegaTableName, Optional<PravegaStreamDescription>> tableCache;

    // whether we have listed tables from this schema or not
    private final HashMap<String, Boolean> tableListMap = new HashMap<>();

    private final ExecutorService exec;

    private final AtomicBoolean initialFetch = new AtomicBoolean();

    @Inject
    PravegaTableDescriptionSupplier(PravegaConnectorConfig pravegaConnectorConfig,
                                    JsonCodec<PravegaStreamDescription> streamDescriptionCodec) {
        requireNonNull(pravegaConnectorConfig, "pravegaConfig is null");

        // no expire time explicitly set in cache, they are listed periodically and will be overwritten
        this.schemaCache = CacheBuilder.newBuilder().build();

        // after expiration we will rebuild field definitions in case table schema changes
        this.tableCache = CacheBuilder.newBuilder()
                .expireAfterWrite(pravegaConnectorConfig.getTableCacheExpireSecs(), TimeUnit.SECONDS)
                .build(new CacheLoader<PravegaTableName, Optional<PravegaStreamDescription>>() {
                    @Override
                    public Optional<PravegaStreamDescription> load(PravegaTableName pravegaTableName) {
                        PravegaStreamDescription streamDescription = loadTable(pravegaTableName.getSchemaTableName());
                        if (streamDescription == null) {
                            throw new RuntimeException("table " + pravegaTableName.getSchemaTableName().getSchemaName() + "." +
                                    pravegaTableName.getSchemaTableName().getTableName() + " not found");
                        }
                        return Optional.of(streamDescription);
                    }
                });

        this.schemaRegistry = new CompositeSchemaRegistry(pravegaConnectorConfig, streamDescriptionCodec);

        this.exec = Executors.newSingleThreadExecutor();
        this.exec.submit(() -> {
            long sleepMs = 2000; // small sleep until we have successful run, then will be bumped up
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    fillSchemaCache();

                    if (!initialFetch.get()) {
                        synchronized (initialFetch) {
                            initialFetch.set(true);
                            initialFetch.notifyAll();
                        }
                    }

                    // we have a successful run, fall back to normal interval
                    sleepMs = pravegaConnectorConfig.getTableCacheExpireSecs() * 1000L;
                } finally {
                    try {
                        Thread.sleep(sleepMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        });
    }

    @VisibleForTesting
    public PravegaTableDescriptionSupplier(CompositeSchemaRegistry schemaRegistry) {
        this.exec = null;
        this.schemaRegistry = schemaRegistry;

        this.schemaCache = CacheBuilder.newBuilder().build();
        this.tableCache = CacheBuilder.newBuilder().build(new CacheLoader<PravegaTableName, Optional<PravegaStreamDescription>>() {
            @Override
            public Optional<PravegaStreamDescription> load(PravegaTableName pravegaTableName) {
                PravegaStreamDescription streamDescription = loadTable(pravegaTableName.getSchemaTableName());
                return streamDescription == null ? Optional.empty() : Optional.of(streamDescription);
            }
        });

        fillSchemaCache();

        this.initialFetch.set(true);
    }

    private void fillSchemaCache()
    {
        log.info("refill table cache");
        for (String schema : schemaRegistry.listSchemas()) {
            List<PravegaTableName> finalTables = new ArrayList<>();

            List<Pattern> compositeStreams = new ArrayList<>();

            schemaRegistry.listTables(schema).forEach(table -> {
                // we hide component streams (components of multi-source streams) from view
                // multi source streams guaranteed to appear first, so compositeStreams will be complete
                // before we actually get to a hidden (component) stream
                boolean hidden =
                        compositeStreams.stream().anyMatch(p -> p.matcher(table.getTableName()).matches());

                finalTables.add(new PravegaTableName(schema, table.getTableName(), hidden));

                if (multiSourceStream(table)) {
                    // if component streams specified look for exact match when hiding
                    if (table.getObjectArgs().isPresent()) {
                        table.getObjectArgs().get().forEach(arg -> {
                            compositeStreams.add(Pattern.compile("^" + arg + "$"));
                        });
                    } else {
                        // regex, fuzzy match
                        compositeStreams.add(Pattern.compile(table.getObjectName()));
                    }
                }
            });

            schemaCache.put(schema, finalTables);
        }
    }

    private void waitInitialSchemaFetch()
    {
        if (initialFetch.get()) {
            return;
        }

        try {
            synchronized (initialFetch) {
                while (!initialFetch.get()) {
                    initialFetch.wait();
                }
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public List<String> listSchemas()
    {
        waitInitialSchemaFetch();

        return schemaCache.asMap().keySet().stream().collect(Collectors.toList());
    }

    public List<PravegaTableName> listTables(Optional<String> schema)
    {
        waitInitialSchemaFetch();

        List<String> schemas = schema.map(Collections::singletonList).orElseGet(this::listSchemas);

        List<PravegaTableName> tableList = new ArrayList<>();
        for (String s : schemas) {
            tableList.addAll(schemaCache.getIfPresent(s));
        }
        return tableList;
    }

    public PravegaStreamDescription getTable(SchemaTableName schemaTableName)
    {
        waitInitialSchemaFetch();

        PravegaTableName pravegaTableName = new PravegaTableName(schemaTableName);
        Optional<PravegaStreamDescription> cachedTable = tableCache.getUnchecked(pravegaTableName);
        if (cachedTable != null && cachedTable.isPresent()) {
            log.debug("serving getTable(%s) from cache", schemaTableName);
            return cachedTable.get();
        }
        else {
            log.debug("could not retrieve definition for getTable(%s)", schemaTableName);
            return null;
        }
    }

    private PravegaStreamDescription loadTable(SchemaTableName schemaTableName)
    {
        PravegaStreamDescription table = schemaRegistry.getTable(schemaTableName);
        if (table == null) {
            return null;
        }

        if (multiSourceStream(table)) {
            // if component streams not already specified, look them up from pravega based on regex
            List<String> objectArgs = table.getObjectArgs().isPresent()
                    ? table.getObjectArgs().get()
                    : multiSourceStreamComponents(schemaTableName, table.getObjectName());
            if (objectArgs.isEmpty()) {
                throw new IllegalArgumentException("could not get component streams for " + schemaTableName);
            }

            List<PravegaStreamFieldGroup> fieldGroups = table.getEvent().orElse(new ArrayList<>(1));
            if (fieldGroups.isEmpty()) {
                fieldGroups = schemaRegistry.getSchema(new SchemaTableName(schemaTableName.getSchemaName(), objectArgs.get(0)));
            }

            table = new PravegaStreamDescription(table, fieldGroups, objectArgs);
        } else if (!fieldsDefined(table)) {
            table = new PravegaStreamDescription(table, schemaRegistry.getSchema(schemaTableName));
        }

        return table;
    }

    private List<String> multiSourceStreamComponents(SchemaTableName schemaTableName, String sourcePattern)
    {
        Pattern pattern = Pattern.compile(sourcePattern);

        return listTables(Optional.of(schemaTableName.getSchemaName())).stream()
                .map(PravegaTableName::getSchemaTableName)
                .map(SchemaTableName::getTableName)
                .filter(tableName -> pattern.matcher(tableName).matches())
                .collect(Collectors.toList());
    }

    private static boolean fieldsDefined(PravegaStreamDescription table)
    {
        if (!table.getEvent().isPresent() ||
                table.getEvent().get().isEmpty()) {
            return false;
        }

        for (PravegaStreamFieldGroup fieldGroup : table.getEvent().get()) {
            if (fieldGroup.getFields() == null) {
                return false;
            }

        }
        return true;
    }
}
