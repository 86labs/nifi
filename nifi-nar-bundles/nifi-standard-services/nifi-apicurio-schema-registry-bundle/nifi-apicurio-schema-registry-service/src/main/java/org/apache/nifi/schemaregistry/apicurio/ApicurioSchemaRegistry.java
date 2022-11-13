package org.apache.nifi.schemaregistry.apicurio;

import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.rest.client.RegistryClientFactory;
import io.apicurio.registry.rest.client.exception.ArtifactNotFoundException;
import io.apicurio.registry.rest.client.exception.VersionNotFoundException;
import io.apicurio.registry.rest.v2.beans.ArtifactMetaData;
import io.apicurio.registry.rest.v2.beans.VersionMetaData;
import io.apicurio.registry.types.ArtifactType;
import org.apache.avro.Schema;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

@Tags({"schema","registry","avro","apicurio"})
@CapabilityDescription("Provides a Schema Registry Service that interacts with an Apicurio Schema Registry, available at https://www.apicur.io/registry/" +
                       "This implementation uses the branch field to specify the Apicurio groupId." +
                       "Currently only AVRO schemas are supported.")
public class ApicurioSchemaRegistry extends AbstractControllerService implements SchemaRegistry {

    private static final Set<SchemaField> schemaFields = EnumSet.of(SchemaField.SCHEMA_NAME,
            SchemaField.SCHEMA_IDENTIFIER,
            SchemaField.SCHEMA_BRANCH_NAME,
            SchemaField.SCHEMA_TEXT,
            SchemaField.SCHEMA_TEXT_FORMAT,
            SchemaField.SCHEMA_VERSION,
            SchemaField.SCHEMA_VERSION_ID);

    public static final PropertyDescriptor URL = new PropertyDescriptor.Builder()
            .name("url")
            .displayName("Schema Registry URL")
            .description("URL of the schema registry that this Controller Service should connect to, including version. For example, http://localhost:8080/apis/registry/v2")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();

    private volatile RegistryClient schemaRegistryClient;
    private volatile String baseUrl;
    private volatile boolean initialized;
    private volatile Map<String,Object> schemaRegistryConfig;

    protected synchronized RegistryClient getClient() {
        if(!initialized) {
            schemaRegistryClient = RegistryClientFactory.create(baseUrl);
            initialized = true;
        }
        return schemaRegistryClient;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(URL);
        return properties;
    }

    @OnEnabled
    public void enable(final ConfigurationContext context) throws InitializationException {
        String urlValue = context.getProperty(URL).evaluateAttributeExpressions().getValue();
        if(urlValue == null || urlValue.trim().isEmpty()) {
            throw new IllegalArgumentException("'Schema Registry URL' must not be null or empty.");
        }
        baseUrl = urlValue;
        schemaRegistryConfig = new HashMap<>();
        // TODO: Fill in configs that are necessary
    }

    @OnDisabled
    public void close(){
        if (schemaRegistryClient != null) {
            try {
                schemaRegistryClient.close();
            } catch (IOException e) {
                getLogger().error("could not close schema registry client", e);
            }
        }
        initialized = false;
    }



    private SchemaIdentifier buildSchemaIdentifierFromMetadata(ArtifactMetaData metadata,
                                                               VersionMetaData versionMetaData,
                                                               String groupId) {
        getLogger().info("Got metadata " + metadata);
        getLogger().info("Got version metadata " + versionMetaData);
        SchemaIdentifier identifier;
        try {
            Integer versionInt = Integer.parseInt(metadata.getVersion());
            identifier = SchemaIdentifier.builder()
                    .name(metadata.getName())
                    .groupId(groupId)
                    .branch(metadata.getVersion())
                    .id(metadata.getId())
                    .version(versionInt)
                    .schemaVersionId(versionMetaData.getGlobalId())
                    .build();
        } catch (NumberFormatException ex) {
            identifier = SchemaIdentifier.builder()
                    .name(metadata.getName())
                    .groupId(groupId)
                    .branch(metadata.getVersion())
                    .id(metadata.getId())
                    .schemaVersionId(versionMetaData.getGlobalId())
                    .build();
        }
        return identifier;
    }
    private RecordSchema retrieveSchemaByGroupAndName(SchemaIdentifier schemaIdentifier) throws SchemaNotFoundException {
        final RegistryClient client = getClient();

        final Optional<String> schemaNameOpt = schemaIdentifier.getName();
        final Optional<String> groupIdOpt = schemaIdentifier.getGroupId();
        final Optional<String> versionOpt;
        if(schemaIdentifier.getVersion().isPresent()) {
            versionOpt = schemaIdentifier.getVersion().stream().mapToObj(Integer::toString).findFirst();;
        } else {
            versionOpt = schemaIdentifier.getBranch();
        }

        schemaIdentifier.getBranch();

        if (!schemaNameOpt.isPresent()) {
            throw new SchemaNotFoundException("Cannot retrieve schema because Schema Name is not present");
        }
        if (!groupIdOpt.isPresent()) {
            throw new SchemaNotFoundException("Cannot retrieve schema because Group ID is not present");
        }

        try {
            ArtifactMetaData metadata = client.getArtifactMetaData(groupIdOpt.get(), schemaNameOpt.get());
            VersionMetaData versionMetaData;
            InputStream schemaInputStream;
            if (versionOpt.isPresent()) {
                versionMetaData = client.getArtifactVersionMetaData(groupIdOpt.get(), schemaNameOpt.get(), versionOpt.get());
                schemaInputStream = client.getArtifactVersion(groupIdOpt.get(), schemaNameOpt.get(), versionOpt.get());
            } else {
                versionMetaData = client.getArtifactVersionMetaData(groupIdOpt.get(), schemaNameOpt.get(), metadata.getVersion());
                schemaInputStream = client.getLatestArtifact(groupIdOpt.get(), schemaNameOpt.get());
            }

            SchemaIdentifier returnedSchemaIdentifier = buildSchemaIdentifierFromMetadata(metadata, versionMetaData, groupIdOpt.get());
            String schemaString = new String(schemaInputStream.readAllBytes());
            final Schema schema = new Schema.Parser().parse(schemaString);
            return AvroTypeUtil.createSchema(schema, schemaString, returnedSchemaIdentifier);
        } catch (final ArtifactNotFoundException ex){
            String errorMsg = String.format("No schema was found for group %s artifact %s",
                                                groupIdOpt.get(), schemaNameOpt.get());
            throw new SchemaNotFoundException(errorMsg);
        } catch (final VersionNotFoundException ex){
            String errorMsg = String.format("No schema was found for group %s artifact %s version %s",
                    groupIdOpt.get(), schemaNameOpt.get(), versionOpt.get());
            throw new SchemaNotFoundException(errorMsg);
        } catch (final Exception ex) {
            getLogger().error("Could not create the schema or find it", ex);
            return null;
        }
    }

    @Override
    public RecordSchema retrieveSchema(SchemaIdentifier schemaIdentifier) throws  SchemaNotFoundException {
        return retrieveSchemaByGroupAndName(schemaIdentifier);
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return schemaFields;
    }
}
