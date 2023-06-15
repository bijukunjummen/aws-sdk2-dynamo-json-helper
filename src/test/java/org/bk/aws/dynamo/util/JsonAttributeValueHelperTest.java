package org.bk.aws.dynamo.util;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import static org.assertj.core.api.Assertions.assertThat;

class JsonAttributeValueHelperTest {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void convertSimpleObject() throws Exception {
        String json = "{\n" +
                "  \"key1\": \"value1\",\n" +
                "  \"key2\": 1.345,\n" +
                "  \"key3\": 42,\n" +
                "  \"key4\": [\n" +
                "    \"a\",\n" +
                "    \"b\",\n" +
                "    \"c\"\n" +
                "  ],\n" +
                "  \"key5\": null\n" +
                "}";

        JsonNode jsonNode = objectMapper.readTree(json);
        AttributeValue attributeValue = JsonAttributeValueUtil.toAttributeValue(jsonNode);

        assertThat(attributeValue.hasM())
                .as("json object should get mapped to an attribute with map")
                .isTrue();

        assertThat(attributeValue.m())
                .as("root object should have a key key1, with an string attribute value1")
                .containsEntry("key1", AttributeValue.builder().s("value1").build());

        JSONAssert.assertEquals(json,
                objectMapper.writeValueAsString(JsonAttributeValueUtil.fromAttributeValue(attributeValue)),
                true);

        AttributeValue attributeValueViaRawJson = JsonAttributeValueUtil.toAttributeValue(json, objectMapper);
        assertThat(attributeValueViaRawJson).isEqualTo(attributeValue);
    }

    @Test
    void convertRootList() throws Exception {
        String json = "[\n" +
                "  \"a\",\n" +
                "  \"b\",\n" +
                "  \"c\",\n" +
                "  {\n" +
                "    \"somekey\": \"someval\"\n" +
                "  }\n" +
                "]";

        JsonNode jsonNode = objectMapper.readTree(json);
        AttributeValue attributeValue = JsonAttributeValueUtil.toAttributeValue(jsonNode);

        assertThat(attributeValue.hasM())
                .as("attribute value should not have a root object. It should have a root list")
                .isFalse();

        assertThat(attributeValue.hasL())
                .as("Attribute value should have a root list")
                .isTrue();

        assertThat(attributeValue.l())
                .as("root object should have a key key1, with an string attribute value1")
                .contains(
                        AttributeValue.builder().s("a").build(),
                        AttributeValue.builder().s("b").build(),
                        AttributeValue.builder().s("c").build());

        JSONAssert.assertEquals(json,
                objectMapper.writeValueAsString(JsonAttributeValueUtil.fromAttributeValue(attributeValue)),
                true);
    }

    @Test
    void handlingNumericTypes() throws Exception {
        String json = "{\n" +
                "  \"key1\": 15,\n" +
                "  \"key2\": 1.345,\n" +
                "  \"key3\": true,\n" +
                "  \"key5\": null\n," +
                "  \"key6\": -0.5000\n," +
                "}";

        JsonNode jsonNode = objectMapper.readTree(json);
        AttributeValue attributeValue = JsonAttributeValueUtil.toAttributeValue(jsonNode);

        assertThat(attributeValue.hasM()).isTrue();

        JSONAssert.assertEquals(json,
                objectMapper.writeValueAsString(JsonAttributeValueUtil.fromAttributeValue(attributeValue)),
                true);
    }
}
