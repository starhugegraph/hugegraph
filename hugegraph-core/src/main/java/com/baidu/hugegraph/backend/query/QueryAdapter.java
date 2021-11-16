package com.baidu.hugegraph.backend.query;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.query.Condition;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

public class QueryAdapter implements JsonSerializer<Condition>, JsonDeserializer<Condition> {

    @Override
    public Condition deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        JsonObject object = json.getAsJsonObject();
        String type = object.get("clazz").getAsString();
        JsonElement element = object.get("element");
        try {
            return context.deserialize(element, Class.forName(type));
        } catch (Exception e) {
            throw new BackendException("Unknown element type: " + type, e);
        }
    }

    @Override
    public JsonElement serialize(Condition src, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject result = new JsonObject();
        result.add("clazz", new JsonPrimitive(src.getClass().getName()));
        result.add("element", context.serialize(src, src.getClass()));
        return result;
    }
}

