package org.apache.druid.server.grpc.common;

import java.util.List;

/** Technology-independent query schema */
public final class QuerySchemas {
    public static final class QuerySchema {
        public final List<String> dimensions;
        public final List<QuerySchemaMetric> metrics;

        public QuerySchema(List<String> dimensions, List<QuerySchemaMetric> metrics) {
            this.dimensions = dimensions;
            this.metrics = metrics;
        }
    }

    public static final class QuerySchemaMetric {
        public final String name;
        public final MetricType type;

        public QuerySchemaMetric(String name, MetricType type) {
            this.name = name;
            this.type = type;
        }
    }

    // Druid MetricHolder is not public enough :(
    public enum MetricType {
        LONG,
        DOUBLE;

        public static MetricType determineType(String typeName) {
            if ("long".equalsIgnoreCase(typeName)) {
                return LONG;
            } else if ("double".equalsIgnoreCase(typeName)) {
                return DOUBLE;
            }

            throw new IllegalArgumentException("Unsupported metric type: " + typeName);
        }
    }
}
