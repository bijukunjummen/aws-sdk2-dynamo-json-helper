package org.bk.aws.dynamo.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class JsonAttributeValueUtil {

    private JsonAttributeValueUtil() {

    }

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

    private static JsonNode fromAttributeValue(Map<String, AttributeValue> map) {
        ObjectNode objectNode = JsonNodeFactory.instance.objectNode();
        map.entrySet().forEach(entry -> objectNode.set(entry.getKey(), fromAttributeValue(entry.getValue())));
        return objectNode;
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

