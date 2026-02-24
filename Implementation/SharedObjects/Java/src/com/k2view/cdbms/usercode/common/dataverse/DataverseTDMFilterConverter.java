package com.k2view.cdbms.usercode.common.dataverse;

import java.util.*;
import java.sql.*;
import java.math.*;
import java.io.*;
import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.sync.*;
import com.k2view.fabric.common.Json;
import com.k2view.fabric.common.ParamConvertor;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;

public class DataverseTDMFilterConverter {
	@SuppressWarnings("unchecked")
    static String convertToODataFilter(String filter) {
        Map<String, Object> filterMap = Json.get().fromJson(filter);

        if (filterMap == null || !filterMap.containsKey("group")) {
            return "";
        }

        return processGroup((Map<String, Object>) filterMap.get("group"));
    }

    private static String processGroup(Map<String, Object> group) {
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> rules = (List<Map<String, Object>>) group.get("rules");

        if (rules.isEmpty())
            return "";

        StringBuilder filterBuilder = new StringBuilder();

        for (int i = 0; i < rules.size(); i++) {
            Map<String, Object> rule = rules.get(i);
            @SuppressWarnings("unchecked")
            String filter = rule.containsKey("group")
                    ? "(" + processGroup((Map<String, Object>) rule.get("group")) + ")"
                    : processRule(rule);

            if (filterBuilder.length() > 0) {
                String operator = (String) rules.get(i - 1).get("operator");
                if ("OR".equalsIgnoreCase(operator)) {
                    filterBuilder.append(" or ");
                } else {
                    filterBuilder.append(" and ");
                }
            }

            filterBuilder.append(filter);
        }

        return filterBuilder.toString();
    }

    private static String processRule(Map<String, Object> rule) {
        String field = rule.get("field").toString();
        String condition = rule.get("condition").toString().toUpperCase();
        Object value = rule.get("data");
        String type = (String) rule.get("original_type");

        // Numeric conversion if needed
        if (!condition.contains("IN") && value instanceof String s
                && List.of("NUMBER", "REAL", "INTEGER").contains(type)) {
            value = ParamConvertor.toNumber(s);
        }

        return switch (condition) {
            case "=" -> field + " eq " + formatValue(value, type);
            case "!=", "<>" -> field + " ne " + formatValue(value, type);
            case ">" -> field + " gt " + formatValue(value, type);
            case ">=" -> field + " ge " + formatValue(value, type);
            case "<" -> field + " lt " + formatValue(value, type);
            case "<=" -> field + " le " + formatValue(value, type);
            case "IN" -> expandIn(field, (String) value, rule, false);
            case "NOT IN" -> expandIn(field, (String) value, rule, true);
            case "IS NULL" -> field + " eq null";
            case "IS NOT NULL" -> field + " ne null";
            default -> throw new IllegalArgumentException("Unsupported condition: " + condition);
        };
    }

    private static String expandIn(String field, String value, Map<String, Object> rule, boolean negate) {
        List<String> comparisons = new ArrayList<>();

        for (String item : value.split(",")) {
            item = item.trim();
            String formattedValue;

            if ("NUMBER".equalsIgnoreCase((String) rule.get("original_type"))) {
                formattedValue = item;
            } else {
                formattedValue = "'" + item.replace("'", "''") + "'";
            }

            String comparison = negate
                    ? field + " ne " + formattedValue
                    : field + " eq " + formattedValue;

            comparisons.add(comparison);
        }

        String joined = String.join(negate ? " and " : " or ", comparisons);
        return comparisons.size() > 1 ? "(" + joined + ")" : joined;
    }

    private static String formatValue(Object value, String type) {
        if (value == null) {
            return "null";
        }
        if (List.of("NUMBER", "REAL", "INTEGER").contains(type)) {
            return value.toString();
        }
        String str = value.toString();
        str = str.replace("'", "''"); // Escape single quotes
        return "'" + str + "'";
    }
}

