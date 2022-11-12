package org.apache.nifi.schemaregistry.apicurio;


import io.apicurio.registry.rest.v2.beans.ArtifactSearchResults;
import io.apicurio.registry.rest.v2.beans.SearchedArtifact;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.schemaregistry.apicurio.containers.ApicurioTestingBase;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;

@Testcontainers
public class TestApiCurioBasicContainer extends ApicurioTestingBase {

    private ApicurioSchemaRegistry testSubject;
    private TestRunner runner;
    private String controllerServiceId = "apicurio-schema-registry";
    @Mock
    private Processor dummyProcessor;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        testSubject = new ApicurioSchemaRegistry();
        runner = TestRunners.newTestRunner(dummyProcessor);
        runner.addControllerService(controllerServiceId, testSubject);
    }

    @Test
    void validWhenUsingBasicTestContainer() throws Exception {
        runner.setProperty(testSubject, ApicurioSchemaRegistry.URL.getName(), apiCurioContainer.getRegistryUrl());
        runner.enableControllerService(runner.getControllerService(controllerServiceId));
        ApicurioSchemaRegistry registry = (ApicurioSchemaRegistry) runner.getControllerService(controllerServiceId);
        ArtifactSearchResults listing = registry.getClient().listArtifactsInGroup("default");
        List<SearchedArtifact> artifacts = listing.getArtifacts();
        Assertions.assertEquals(0,artifacts.size());
    }
}
