package org.bk.aws.dynamo.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.skyscreamer.jsonassert.JSONAssert;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;

import java.net.URI;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class JsonAttributeValueDynamoIntegrationTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @RegisterExtension
    static TestContainerDynamoDBExtension dynamoDBExtension = new TestContainerDynamoDBExtension();


    private static DynamoDbClient getClient() {
        DynamoDbClientBuilder builder = DynamoDbClient.builder()
                .endpointOverride(URI.create(dynamoDBExtension.getEndpoint()))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider
                        .create(AwsBasicCredentials
                                .create("acc", "sec")));

        return builder.build();
    }

    @Test
    void saveAndRetrieveJson() throws Exception {
        DynamoDbClient dynamoDbClient = getClient();
        dynamoDbClient.createTable(
                CreateTableRequest.builder()
                        .tableName("testtable")
                        .attributeDefinitions(AttributeDefinition.builder()
                                .attributeName("id")
                                .attributeType(ScalarAttributeType.S)
                                .build())
                        .keySchema(KeySchemaElement.builder()
                                .attributeName("id")
                                .keyType(KeyType.HASH)
                                .build())
                        .provisionedThroughput(ProvisionedThroughput.builder()
                                .readCapacityUnits(10L)
                                .writeCapacityUnits(10L)
                                .build())
                        .build());

        DescribeTableRequest describeTableRequest = DescribeTableRequest.builder()
                .tableName("testtable")
                .build();

        DescribeTableResponse describeTableResponse = dynamoDbClient.describeTable(describeTableRequest);
        if (describeTableResponse.table().tableStatus() != TableStatus.ACTIVE) {
            throw new RuntimeException("Table not ready!");
        }

        //language=JSON
        String json =
                "{\n" +
                        "  \"key1\": \"value1\",\n" +
                        "  \"key2\": [\n" +
                        "    \"arr1\",\n" +
                        "    \"arr2\"\n" +
                        "  ],\n" +
                        "  \"level1\": {\n" +
                        "    \"level2\": {\n" +
                        "      \"level3\": {\n" +
                        "        \"l3Key\": \"l3Value\"\n" +
                        "      }\n" +
                        "    }\n" +
                        "  " +
                        "}\n" +
                        "}\n";

        JsonNode jsonNode = objectMapper.readTree(json);
        AttributeValue dataAttributeValue = JsonAttributeValueUtil.toAttributeValue(jsonNode);

        dynamoDbClient.putItem(PutItemRequest.builder()
                .tableName("testtable")
                .item(
                        Map.of("id", AttributeValue.builder().s("1").build(),
                                "data", dataAttributeValue)
                ).build());

        GetItemResponse getItemResponse = dynamoDbClient.getItem(GetItemRequest.builder()
                .tableName("testtable")
                .key(Map.of("id", AttributeValue.builder().s("1").build()))
                .build()
        );

        assertThat(getItemResponse.item().get("id").s()).isEqualTo("1");
        JsonNode dataJsonNode = JsonAttributeValueUtil.fromAttributeValue(getItemResponse.item().get("data"));
        JSONAssert.assertEquals(json, objectMapper.writeValueAsString(dataJsonNode), true);
    }


}