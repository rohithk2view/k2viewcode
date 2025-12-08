package com.k2view.cdbms.usercode.common.mongodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.bson.conversions.Bson;

import com.k2view.fabric.common.ParamConvertor;
import com.k2view.graphIt.Json;
import com.mongodb.client.model.Filters;

@SuppressWarnings("unchecked")
class MongoTdmFilterConverter {
    static Bson convertToMongoFilter(String filter) {
        Map<String, Object> filterMap = Json.from(filter, Map.class);
        if (filterMap == null || !filterMap.containsKey("group")) {
            return Filters.empty();
        }
        return processGroup((Map<String, Object>) filterMap.get("group"));
    }

    private static Bson processGroup(Map<String, Object> group) {
        List<Map<String, Object>> rules = (List<Map<String, Object>>) group.get("rules");
        if (rules.isEmpty())
            return Filters.empty();

        Bson currentFilter = null;

        for (int i = 0; i < rules.size(); i++) {
            Map<String, Object> rule = rules.get(i);
            Bson filter = rule.containsKey("group") ? processGroup((Map<String, Object>) rule.get("group"))
                    : processRule(rule);

            if (currentFilter == null) {
                // Initialize first filter
                currentFilter = filter;
            } else {
                // Use operator from the previous rule
                String operator = (String) rules.get(i - 1).get("operator");
                if ("OR".equalsIgnoreCase(operator)) {
                    currentFilter = Filters.or(currentFilter, filter);
                } else {
                    currentFilter = Filters.and(currentFilter, filter);
                }
            }
        }

        return currentFilter != null ? currentFilter : Filters.empty();
    }

    private static Bson processRule(Map<String, Object> rule) {
        String field = rule.get("field").toString();
        String condition = rule.get("condition").toString().toUpperCase();
        Object value = rule.get("data");
        String type = (String) rule.get("original_type");
        if (!condition.contains("IN") && value instanceof String s
                && List.of("NUMBER", "REAL", "INTEGER").contains(type)) {
            value = ParamConvertor.toNumber(s);
        }

        return switch (condition) {
            case "=" -> Filters.eq(field, value);
            case "!=", "<>" -> Filters.ne(field, value);
            case ">" -> Filters.gt(field, value);
            case ">=" -> Filters.gte(field, value);
            case "<" -> Filters.lt(field, value);
            case "<=" -> Filters.lte(field, value);
            case "IN" -> Filters.in(field, parseList((String) value, rule));
            case "NOT IN" -> Filters.nin(field, parseList((String) value, rule));
            case "IS NULL" -> Filters.eq(field, null);
            case "IS NOT NULL" -> Filters.ne(field, null);
            default -> throw new IllegalArgumentException("Unsupported condition: " + condition);
        };
    }

    private static Iterable<?> parseList(String value, Map<String, Object> rule) {
        // Split by comma and trim each item
        List<Object> result = new ArrayList<>();
        for (String item : value.split(",")) {
            item = item.trim();
            if ("NUMBER".equalsIgnoreCase((String) rule.get("original_type"))) {
                result.add(ParamConvertor.toNumber(item));
            } else {
                result.add(item);
            }
        }
        return result;
    }
}
