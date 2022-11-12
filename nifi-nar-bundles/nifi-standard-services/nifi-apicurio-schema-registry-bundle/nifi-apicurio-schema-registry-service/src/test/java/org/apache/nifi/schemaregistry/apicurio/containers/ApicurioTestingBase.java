package org.apache.nifi.schemaregistry.apicurio.containers;

import io.apicurio.registry.types.ArtifactType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class ApicurioTestingBase {
    public final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
    public static final ApicurioRegistryContainer apiCurioContainer = new ApicurioRegistryContainer(ApicurioRegistryContainer.defaultImageName);

    private static final Map<String, ArtifactType> extensionToArtifactType = Map.ofEntries(Map.entry("avsc", ArtifactType.AVRO));

    static {
        apiCurioContainer.start();
    }


}
