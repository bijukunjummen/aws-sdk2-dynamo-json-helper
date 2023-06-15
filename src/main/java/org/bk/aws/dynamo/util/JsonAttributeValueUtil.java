package org.bk.aws.dynamo.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility to convert a Jackson {@link JsonNode} to a AWS SDK 2 {@link AttributeValue}
 * and back
 * <p>
 * Uses: It is possible to store a raw string into DynamoDB but then all the advantages of a document store is lost
 *
 * Every processing on that data will have to be done in the application layer by de-serializing the raw string to a
 * json.
 * </p>
 * Storing the json as a nested AttributeValue provides a way for the data to become queryable, nested attributes can be
 * queried, filtered and treated like any other attribute in dynamo.
 */
public final class JsonAttributeValueUtil {
    private JsonAttributeValueUtil() {
    }

    /**
     * Convert a {@link JsonNode} to an {@link AttributeValue}
     *
     * @param jsonNode json represented as a Jackson {@link JsonNode}
     * @return attribute value.
     * @throws IllegalStateException if a attribute type does not easily transform to an AttributeValue type
     */
    public static AttributeValue toAttributeValue(JsonNode jsonNode) {
        if (jsonNode.isObject()) {
            return toAttributeValue((ObjectNode) jsonNode);
        } else if (jsonNode.isArray()) {
            return toAttributeValue((ArrayNode) jsonNode);
        } else if (jsonNode.isValueNode()) {
            return toAttributeValue((ValueNode) jsonNode);
        }
        throw new IllegalStateException("Unexpected node type " + jsonNode.toString());
    }

    /**
     * Convert a raw json to an {@link AttributeValue}
     *
     * @param json         raw json that is internally transformed to a {@link JsonNode}
     * @param objectMapper Jackson {@link ObjectMapper} for converting the raw json to a {@link JsonNode}
     * @return attribute value.
     * @throws IllegalStateException if a attribute type does not easily transform to an AttributeValue type
     */
    public static AttributeValue toAttributeValue(String json, ObjectMapper objectMapper) {
        return toAttributeValue(rawJsonToJsonNode(json, objectMapper));
    }

    /**
     * Convert an  {@link AttributeValue} to a {@link JsonNode}
     *
     * @param attributeValue {@link AttributeValue}
     * @return Json represented as a Jackson {@link JsonNode}
     * @throws IllegalStateException if a json type does not map to an {@link AttributeValue}
     */
    public static JsonNode fromAttributeValue(AttributeValue attributeValue) {
        if (attributeValue.hasM()) {
            return fromAttributeValue(attributeValue.m());
        } else if (attributeValue.hasL()) {
            return fromAttributeValue(attributeValue.l());
        } else if (attributeValue.s() != null) {
            return JsonNodeFactory.instance.textNode(attributeValue.s());
        } else if (attributeValue.bool() != null) {
            return JsonNodeFactory.instance.booleanNode(attributeValue.bool());
        } else if (attributeValue.n() != null) {
            try {
                Number n = NumberFormat.getInstance().parse(attributeValue.n());
                return fromAttributeValue(n);
            } catch (ParseException e) {
                throw new IllegalStateException("Invalid number: " + attributeValue.n());
            }
        } else if (attributeValue.nul()) { //holds a null value
            return JsonNodeFactory.instance.nullNode();
        }

        throw new IllegalStateException("Unexpected attribute value type : " + attributeValue);
    }

    /**
     * Convert a map of {@link AttributeValue} to a {@link JsonNode}
     *
     * @param map of names to {@link AttributeValue}
     * @return Json represented as a Jackson {@link JsonNode}
     * @throws IllegalStateException if a json type does not map to an {@link AttributeValue}
     */
    public static JsonNode fromAttributeValue(Map<String, AttributeValue> map) {
        ObjectNode objectNode = JsonNodeFactory.instance.objectNode();
        map.entrySet().forEach(entry -> objectNode.set(entry.getKey(), fromAttributeValue(entry.getValue())));
        return objectNode;
    }

    private static JsonNode rawJsonToJsonNode(String json, ObjectMapper objectMapper) {
        try {
            return objectMapper.readTree(json);
        } catch (IOException e) {
            throw new IllegalStateException("Invalid Json Provided");
        }
    }

    private static AttributeValue toAttributeValue(ObjectNode objectNode) {
        Map<String, AttributeValue> attributesMap = new HashMap<>();
        Iterable<Map.Entry<String, JsonNode>> attributesIterable = () -> objectNode.fields();
        for (Map.Entry<String, JsonNode> entry : attributesIterable) {
            attributesMap.put(entry.getKey(), toAttributeValue(entry.getValue()));
        }
        return AttributeValue.builder().m(attributesMap).build();
    }

    private static AttributeValue toAttributeValue(ArrayNode arrayNode) {
        AttributeValue.Builder builder = AttributeValue.builder();
        List<AttributeValue> childAttributes = new ArrayList<>();
        arrayNode.forEach(jsonNode -> childAttributes.add(toAttributeValue(jsonNode)));
        return builder.l(childAttributes).build();
    }

    private static AttributeValue toAttributeValue(ValueNode valueNode) {
        if (valueNode.isNumber()) {
            return toAttributeValue((NumericNode) valueNode);
        } else if (valueNode.isBoolean()) {
            return AttributeValue.builder().bool(valueNode.asBoolean()).build();
        } else if (valueNode.isTextual()) {
            return AttributeValue.builder().s(valueNode.asText()).build();
        } else if (valueNode.isNull()) {
            return AttributeValue.builder().nul(true).build();
        }
        throw new IllegalStateException("Unexpected value type : " + valueNode.toString());
    }

    private static AttributeValue toAttributeValue(NumericNode numericNode) {
        return AttributeValue.builder().n(numericNode.asText()).build();
    }

    private static JsonNode fromAttributeValue(List<AttributeValue> list) {
        ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
        list.forEach(attributeValue -> arrayNode.add(fromAttributeValue(attributeValue)));
        return arrayNode;
    }

    private static JsonNode fromAttributeValue(Number number) {
        if (number instanceof Double) {
            return JsonNodeFactory.instance.numberNode((Double) number);
        } else if (number instanceof Float) {
            return JsonNodeFactory.instance.numberNode((Float) number);
        } else if (number instanceof Long) {
            return JsonNodeFactory.instance.numberNode((Long) number);
        } else if (number instanceof Short) {
            return JsonNodeFactory.instance.numberNode((Short) number);
        } else if (number instanceof Integer) {
            return JsonNodeFactory.instance.numberNode((Integer) number);
        }
        throw new IllegalStateException("Unknown Numeric Type : " + number);
    }
}

