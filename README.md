# Jackson JsonNode to AWS SDK 2 AttributeValue

This is a small java library that provides a utility method to convert a [Jackson](https://github.com/FasterXML/jackson) JsonNode
to an AWS SDK 2.x [AttributeValue](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/dynamodb/model/AttributeValue.html)
and back.

## Using the library:

Add a dependency, for eg. with gradle:

```
implementation("com.github.bijukunjummen:aws-sdk2-dyanamo-json-helper:$VERSION")
```

Now, in the code, given a json which looks like this:

```json
{
  "key1": "value1",
  "key2": [
    "arr1",
    "arr2"
  ],
  "level1": {
    "level2": {
      "level3": {
        "l3Key": "l3Value"
      }
    }
  }
}
```

The Json can be converted to an AttributeValue the following way:

```java
JsonNode jsonNode = objectMapper.readTree(json);
AttributeValue dataAttributeValue = JsonAttributeValueUtil.toAttributeValue(jsonNode);
```

The utility recurses the json structure and creates the nested AttributeValue structure.

Once it is saved to dynamoDB, after retrieving the content, it can be converted back to a json the following way:

```java
AttributeValue attributeValue = response.item().get("data");
JsonNode dataJsonNode = JsonAttributeValueUtil.fromAttributeValue(attributeValue);
```

