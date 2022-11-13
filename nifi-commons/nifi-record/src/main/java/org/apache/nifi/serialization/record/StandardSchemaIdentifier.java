/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.serialization.record;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

public class StandardSchemaIdentifier implements SchemaIdentifier {
    private final Optional<String> name;
    private final Optional<String> identifier;
    private final Optional<String> version;
    private final OptionalLong schemaVersionId;
    private final Optional<String> branch;

    private final Optional<String> groupId;

    StandardSchemaIdentifier(final String name, final String identifier, final String version,
            final Long schemaVersionId, final String branch, final String groupId) {
        this.name = Optional.ofNullable(name);
        this.identifier = Optional.ofNullable(identifier);
        this.version = Optional.ofNullable(version);
        this.schemaVersionId = schemaVersionId == null ? OptionalLong.empty() : OptionalLong.of(schemaVersionId);
        this.branch = Optional.ofNullable(branch);
        this.groupId = Optional.ofNullable(groupId);
    }

    @Override
    public Optional<String> getName() {
        return name;
    }

    @Override
    public Optional<String> getIdentifier() {
        return identifier;
    }

    @Override
    public OptionalLong getIdentifierAsLong() {
        return identifier.stream().mapToLong( n -> Long.parseLong(n)).findFirst();
    }

    @Override
    public Optional<String> getVersion() {
        return version;
    }

    @Override
    public OptionalInt getVersionAsInt() {
        return version.stream().mapToInt( n -> Integer.parseInt(n)).findFirst();
    }

    @Override
    public OptionalLong getSchemaVersionId() {
        return schemaVersionId;
    }

    @Override
    public Optional<String> getBranch() {
        return branch;
    }

    @Override
    public Optional<String> getGroupId() { return groupId; }

    @Override
    public int hashCode() {
        return 31 + 41 * getName().hashCode() + 41 * getIdentifier().hashCode() + 41 * getVersion().hashCode()
                + 41 * getSchemaVersionId().hashCode() + 41 * getBranch().hashCode() + 41 * getGroupId().hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof SchemaIdentifier)) {
            return false;
        }
        final SchemaIdentifier other = (SchemaIdentifier) obj;
        return getName().equals(other.getName())
                && getIdentifier().equals(other.getIdentifier())
                && getVersion().equals(other.getVersion())
                && getSchemaVersionId().equals(other.getSchemaVersionId())
                && getBranch().equals(other.getBranch())
                && getGroupId().equals(other.getGroupId());
    }

    @Override
    public String toString() {
        return "[ name = " + name + ", "
                + "identifier = " + identifier + ", "
                + "version = " + version + ", "
                + "schemaVersionId = " + schemaVersionId + ", "
                + "branch = " + branch + ", "
                + "groupId = " + groupId + " ]";
    }

    /**
     * Builder to create instances of SchemaIdentifier.
     */
    public static class Builder implements SchemaIdentifier.Builder {

        private String name;
        private String branch;
        private String groupId;
        private String identifier;
        private String version;
        private Long schemaVersionId;

        @Override
        public SchemaIdentifier.Builder name(final String name) {
            this.name = name;
            return this;
        }

        @Override
        public SchemaIdentifier.Builder id(final String id) {
            this.identifier = id;
            return this;
        }

        @Override
        public SchemaIdentifier.Builder id(final Long id) {
            this.identifier = String.valueOf(id);
            return this;
        }

        @Override
        public SchemaIdentifier.Builder version(final Integer version) {
            this.version = String.valueOf(version);
            return this;
        }

        @Override
        public SchemaIdentifier.Builder version(final String version) {
            this.version = version;
            return this;
        }
        @Override
        public SchemaIdentifier.Builder branch(final String branch) {
            this.branch = branch;
            return this;
        }

        @Override
        public SchemaIdentifier.Builder groupId(final String groupId) {
            this.groupId = groupId;
            return this;
        }
        @Override
        public SchemaIdentifier.Builder schemaVersionId(final Long schemaVersionId) {
            this.schemaVersionId = schemaVersionId;
            return this;
        }

        @Override
        public SchemaIdentifier build() {
            return new StandardSchemaIdentifier(name, identifier, version, schemaVersionId, branch, groupId);
        }
    }
}
