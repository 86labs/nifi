package org.apache.nifi.schemaregistry.apicurio;

import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.rest.client.RegistryClientFactory;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
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
import java.util.*;

@Tags({"schema","registry","avro","apicurio"})
@CapabilityDescription("Provides a Schema Registry Service that interacts with an Apicurio Schema Registry, available at https://www.apicur.io/registry/")
public class ApicurioSchemaRegistry extends AbstractControllerService implements SchemaRegistry {

    private static final Set<SchemaField> schemaFields = EnumSet.of(SchemaField.SCHEMA_NAME,
            SchemaField.SCHEMA_IDENTIFIER,
            SchemaField.SCHEMA_BRANCH_NAME,
            SchemaField.SCHEMA_TEXT,
            SchemaField.SCHEMA_TEXT_FORMAT,
            SchemaField.SCHEMA_VERSION,
            SchemaField.SCHEMA_VERSION_ID);

    static final PropertyDescriptor URL = new PropertyDescriptor.Builder()
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

    @Override
    public RecordSchema retrieveSchema(SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException {
        OptionalLong l  = schemaIdentifier.getIdentifier();

        return null;
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return schemaFields;
    }
}
