package org.apache.druid.server.grpc.common;

import java.util.List;

/** Technology-independent query schema */
public final class QuerySchemas {
    public static final class QuerySchema {
        public final List<QuerySchemaDimension> dimensions;
        public final List<QuerySchemaMetric> metrics;
        public final boolean hasTimeDimension;

        public QuerySchema(boolean hasTimeDimension, List<QuerySchemaDimension> dimensions, List<QuerySchemaMetric> metrics) {
            this.hasTimeDimension = hasTimeDimension;
            this.dimensions = dimensions;
            this.metrics = metrics;
        }
    }

    public static final class QuerySchemaDimension {
        public final int index;
        public final String name;

        public QuerySchemaDimension(int index, String name) {
            this.index = index;
            this.name = name;
        }
    }

    public static final class QuerySchemaMetric {
        public final int index;
        public final String name;
        public final MetricType type;

        public QuerySchemaMetric(int index, String name, MetricType type) {
            this.index = index;
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
