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
package io.pravega.connectors.presto.schemamanagement;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.annotations.VisibleForTesting;
import io.pravega.connectors.presto.PravegaConnectorConfig;
import io.pravega.connectors.presto.PravegaStreamDescription;
import io.pravega.connectors.presto.PravegaStreamFieldGroup;
import io.pravega.connectors.presto.PravegaTableHandle;

import java.util.*;

public class CompositeSchemaRegistry
        implements SchemaSupplier, SchemaRegistry {
    private final List<SchemaSupplier> schemaSuppliers;

    private final List<SchemaRegistry> schemaRegistries;

    private Set<String> scopes = new HashSet<>();

    public CompositeSchemaRegistry(PravegaConnectorConfig config, JsonCodec<PravegaStreamDescription> streamDescriptionCodec) {
        schemaSuppliers = new ArrayList<>();
        schemaRegistries = new ArrayList<>();

        if (config.getScopes() != null) {
            for (String scope : config.getScopes().split(",")) {
                scopes.add(scope.trim());
            }
        }

        // local will override, always add first
        // if local is not first you may experience subtle issues (mostly related to multisource streams)
        if (config.getTableDescriptionDir() != null &&
                config.getTableDescriptionDir().exists() &&
                config.getTableDescriptionDir().isDirectory()) {
            LocalSchemaRegistry schemaRegistry =
                    new LocalSchemaRegistry(config.getTableDescriptionDir(), streamDescriptionCodec);
            schemaSuppliers.add(schemaRegistry);
            schemaRegistries.add(schemaRegistry);
        }

        if (config.getSchemaRegistryURI() != null) {
            PravegaSchemaRegistry schemaRegistry =
                    new PravegaSchemaRegistry(config.getControllerURI(), config.getSchemaRegistryURI());
            schemaSuppliers.add(schemaRegistry);
            schemaRegistries.add(schemaRegistry);
        }
    }

    @VisibleForTesting
    public CompositeSchemaRegistry(List<SchemaSupplier> schemaSuppliers, List<SchemaRegistry> schemaRegistries)
    {
        this.schemaSuppliers = schemaSuppliers;
        this.schemaRegistries = schemaRegistries;
    }

    private void validatePrestoSchema(String schema)
    {
        if (!scopes.isEmpty() && !scopes.contains(schema)) {
            // TODO: what is the actual exception to throw?
            throw new IllegalArgumentException("schema does not exist " + schema);
        }
    }

    @Override
    public List<String> listSchemas()
    {
        final List<String> schemas = new ArrayList<>();
        if (!scopes.isEmpty()) {
            schemas.addAll(scopes);
        }
        else {
            schemaSuppliers.forEach(p -> schemas.addAll(p.listSchemas()));
        }
        return schemas;
    }

    @Override
    public List<PravegaTableHandle> listTables(String schema)
    {
        validatePrestoSchema(schema);

        Set<PravegaTableHandle> dedupeSet =
                new TreeSet<>(Comparator.comparing(PravegaTableHandle::getTableName));

        final List<PravegaTableHandle> tables = new ArrayList<>();
        schemaSuppliers.forEach(p -> {
            p.listTables(schema).forEach(table -> {
                if (dedupeSet.add(table)) {
                    tables.add(table);
                }
            });
        });
        return tables;
    }

    @Override
    public List<PravegaStreamFieldGroup> getSchema(SchemaTableName schemaTableName)
    {
        validatePrestoSchema(schemaTableName.getSchemaName());

        for (SchemaRegistry schemaRegistry : schemaRegistries) {
            List<PravegaStreamFieldGroup> schema = schemaRegistry.getSchema(schemaTableName);
            if (schema != null) {
                return schema;
            }
        }
        return null;
    }

    @Override
    public PravegaStreamDescription getTable(SchemaTableName schemaTableName)
    {
        validatePrestoSchema(schemaTableName.getSchemaName());

        for (SchemaRegistry schemaRegistry : schemaRegistries) {
            PravegaStreamDescription streamDescription = schemaRegistry.getTable(schemaTableName);
            if (streamDescription != null) {
                return streamDescription;
            }
        }
        return null;
    }
}
