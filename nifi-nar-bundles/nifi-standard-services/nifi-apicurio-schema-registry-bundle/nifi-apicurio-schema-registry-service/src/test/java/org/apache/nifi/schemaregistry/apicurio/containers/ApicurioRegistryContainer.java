package org.apache.nifi.schemaregistry.apicurio.containers;

import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.rest.client.RegistryClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class ApicurioRegistryContainer extends GenericContainer<ApicurioRegistryContainer> {
    private static final int APICURIO_PORT = 8080;
    private RegistryClient registryClient;
    public static DockerImageName defaultImageName = DockerImageName.parse("apicurio/apicurio-registry-mem")
            .withTag("2.3.1.Final");
    public ApicurioRegistryContainer(DockerImageName dockerImageName) {
        super(dockerImageName);

        this.addExposedPort(APICURIO_PORT);
    }

    public RegistryClient getRegistryClient() {
        if(registryClient == null){
            registryClient = RegistryClientFactory.create(getRegistryUrl());
        }
        return  registryClient;
    }

    public String getRegistryUrl() {
        return "http://" + this.getHost() + ":" + this.getMappedPort(APICURIO_PORT) + "/apis/registry/v2";
    }
    @Override
    protected void configure() {

        waitingFor(Wait.forHttp("/ui"));
    }
}
