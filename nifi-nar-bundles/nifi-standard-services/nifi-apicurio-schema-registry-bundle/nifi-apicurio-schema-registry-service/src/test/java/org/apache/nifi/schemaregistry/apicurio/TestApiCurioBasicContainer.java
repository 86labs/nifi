package org.apache.nifi.schemaregistry.apicurio;


import io.apicurio.registry.rest.v2.beans.ArtifactSearchResults;
import io.apicurio.registry.rest.v2.beans.SearchedArtifact;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.apicurio.containers.ApicurioTestingBase;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.InputStream;
import java.util.List;


public class TestApiCurioBasicContainer extends ApicurioTestingBase {


    @BeforeAll
    static void initializeSchemas() {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("schemas/avro/hello.avsc");
        apiCurioContainer.getRegistryClient().createArtifact("default",null,is);
        is = Thread.currentThread().getContextClassLoader().getResourceAsStream("schemas/avro/hello.avsc");
        apiCurioContainer.getRegistryClient().createArtifactVersion("default","hello", "test-2",is);

    }

    @Test
    void validWhenUsingBasicTestContainer() throws Exception {
        ApicurioSchemaRegistry registry = (ApicurioSchemaRegistry) runner.getControllerService(controllerServiceId);
        ArtifactSearchResults listing = registry.getClient().listArtifactsInGroup("default");
        List<SearchedArtifact> artifacts = listing.getArtifacts();
        Assertions.assertEquals(1,artifacts.size());
    }

    @Test
    void shouldGetSchemaUsingBasicTestContainer() throws SchemaNotFoundException {
        String groupId = "default";
        String artifactId = "hello";
        final SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder()
                .name(artifactId).groupId(groupId).version(1).build();
        RecordSchema recordSchema = testSubject.retrieveSchema(schemaIdentifier);
        Assertions.assertEquals("avro", recordSchema.getSchemaFormat().get());
        SchemaIdentifier retrievedSchemaIdentifier = recordSchema.getIdentifier();

        Assertions.assertTrue(schemaIdentifier.getGroupId().isPresent());
        Assertions.assertTrue(schemaIdentifier.getName().isPresent());
        Assertions.assertEquals(schemaIdentifier.getName().get(), retrievedSchemaIdentifier.getName().get());
        Assertions.assertEquals(schemaIdentifier.getGroupId(), retrievedSchemaIdentifier.getGroupId());
        Assertions.assertTrue(retrievedSchemaIdentifier.getBranch().isPresent());
        LOGGER.info("got schema identifier: " + recordSchema.getIdentifier());
        recordSchema.getFields().forEach( f -> LOGGER.info("got field " + f));
    }

    @Test
    void shouldThrowSchemaNotFoundIfVersionDoesNotExist(){
        String groupId = "default";
        String artifactId = "hello";
        Integer version = 1000;
        final SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder()
                .name(artifactId).groupId(groupId).version(version).build();
        Exception exception = Assertions.assertThrows(SchemaNotFoundException.class, () -> {
            testSubject.retrieveSchema(schemaIdentifier);
        });

        String expectedMessage = String.format("No schema was found for group %s artifact %s version %d",
                                                groupId,artifactId,version);
        String actualMessage = exception.getMessage();
        Assertions.assertTrue(actualMessage.contains(expectedMessage));

    }
    @Test
    void shouldThrowSchemaNotFoundIfSchemaDoesNotExist(){
        String groupId = "default";
        String artifactId = "non-existent";
        final SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder()
                .name(artifactId).groupId(groupId).build();
        Exception exception = Assertions.assertThrows(SchemaNotFoundException.class, () -> {
            testSubject.retrieveSchema(schemaIdentifier);
        });

        String expectedMessage = String.format("No schema was found for group %s artifact %s",
                    groupId,artifactId);
        String actualMessage = exception.getMessage();
        Assertions.assertTrue(actualMessage.contains(expectedMessage));

    }
}
