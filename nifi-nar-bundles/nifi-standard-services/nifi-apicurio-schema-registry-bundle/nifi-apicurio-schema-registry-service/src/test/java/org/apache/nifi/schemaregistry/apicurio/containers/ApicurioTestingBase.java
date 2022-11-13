package org.apache.nifi.schemaregistry.apicurio.containers;

import io.apicurio.registry.types.ArtifactType;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.schemaregistry.apicurio.ApicurioSchemaRegistry;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Map;

public abstract class ApicurioTestingBase {
    public final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
    public static final ApicurioRegistryContainer apiCurioContainer = new ApicurioRegistryContainer(ApicurioRegistryContainer.defaultImageName);

    private static final Map<String, ArtifactType> extensionToArtifactType = Map.ofEntries(Map.entry("avsc", ArtifactType.AVRO));

    protected ApicurioSchemaRegistry testSubject;
    protected TestRunner runner;
    protected static final String controllerServiceId = "apicurio-schema-registry";
    @Mock
    private Processor dummyProcessor;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        testSubject = new ApicurioSchemaRegistry();
        runner = TestRunners.newTestRunner(dummyProcessor);
        runner.addControllerService(controllerServiceId, testSubject);
        runner.setProperty(testSubject,ApicurioSchemaRegistry.URL.getName(), apiCurioContainer.getRegistryUrl());
        runner.enableControllerService(runner.getControllerService(controllerServiceId));
    }

    static {
        apiCurioContainer.start();
    }


}
