package org.bk.aws.dynamo.util;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;

/**
 * Spins up an Docker container based DynamoDB for use in JUnit5 based integration tests
 * <p>
 * Use it the following way:
 * <p>
 *
 * <pre>
 *   @RegisterExtension
 *  static TestContainerDynamoDBExtension dynamoDb = new TestContainerDynamoDBExtension();
 * </pre>
 * <p>
 * Use the dynamically generated endpoint from the rule:
 *
 * <pre>
 *     String endpoint = dynamoDb.getEndpoint()
 * </pre>
 */
public final class TestContainerDynamoDBExtension implements BeforeAllCallback, AfterAllCallback {

    private static final String LOCAL_DYNAMODB_IMAGE_NAME = "amazon/dynamodb-local:1.16.0";
    private static final int EXPOSED_PORT = 8000;

    private GenericContainer server;
    private String endpoint;

    @Override
    public void beforeAll(ExtensionContext context) {
        try {
            this.server = new GenericContainer(LOCAL_DYNAMODB_IMAGE_NAME)
                    .withExposedPorts(EXPOSED_PORT)
                    .withCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb")
                    .waitingFor(new HostPortWaitStrategy());

            this.server.start();
            this.endpoint = String.format("http://%s:%d",
                    this.server.getContainerIpAddress(),
                    this.server.getMappedPort(EXPOSED_PORT));

            System.setProperty("aws.accessKeyId", "access-key");
            System.setProperty("aws.secretKey", "secret-key");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void afterAll(ExtensionContext context) {
        if (this.server == null) {
            return;
        }

        try {
            this.server.stop();
            System.clearProperty("aws.accessKeyId");
            System.clearProperty("aws.secretKey");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String getEndpoint() {
        return endpoint;
    }
}
