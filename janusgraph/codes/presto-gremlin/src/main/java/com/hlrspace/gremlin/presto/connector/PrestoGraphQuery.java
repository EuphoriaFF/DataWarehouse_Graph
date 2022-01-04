package com.hlrspace.gremlin.presto.connector;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class PrestoGraphQuery implements Query{

    private String result;
    private PrestoGraphQuery(){}

    private PrestoGraphQuery(Builder builder) {
        if(builder.select) {
            String selectorStr = builder.selectors.stream().reduce((a, b) -> a + ", " + b).get();
            result = "select "
                    + selectorStr
                    + " from "
                    + builder.keyspace
                    + "."
                    + builder.label;

            if (!builder.whereClause.isEmpty())
                result += " where " + builder.whereClause;
            if (!builder.groupByClause.isEmpty())
                result += " group by " + builder.groupByClause;
            if (!builder.orderByClause.isEmpty())
                result += " order by " + builder.orderByClause;
            if (!builder.perPartitionLimitClause.isEmpty())
                result += " per partition limit " + builder.perPartitionLimitClause;
            if (!builder.limitClause.isEmpty())
                result += " limit " + builder.limitClause;
            if (builder.allowFilter)
                result += " ALLOW FILTERING ";
        }
        else if(builder.direct) {
            result = builder.rawQuery;
        }
    }


    @Override
    public String toString() {
        return this.result;
    }

    @Override
    public String query() {
        return result;
    }

    public static Builder builder() {return new Builder();}

    public static final class Builder {
        private boolean select = false;
        private boolean direct = false;
        private String rawQuery = "";
        private String label;
        private String keyspace = ""; //1代表是vertex，0代表是边
        private List<String> selectors = new ArrayList<>();
        private String whereClause = "";
        private String groupByClause = "";
        private String orderByClause = "";
        private String perPartitionLimitClause = "";
        private String limitClause= "";
        private boolean allowFilter = false;

        public Builder select() {
            this.select = true;
            return this;
        }

        public Builder nativeSQL(String raw) {
            this.direct = true;
            this.rawQuery = raw;
            return this;
        }

        public Builder label(String label) {
            this.label = label;
            return this;
        }

        public Builder keyspace(String keyspace) {
            this.keyspace = keyspace;
            return this;
        }

        public Builder selector(String... selectors) {
            for(String e : selectors) {
                this.selectors.add(e);
            }
            return this;
        }

        public Builder where(String whereClause) {
            this.whereClause = whereClause;
            return this;
        }

        public Builder groupBy(String groupByClause) {
            this.groupByClause = groupByClause;
            return this;
        }

        public Builder orderBy(String orderByClause) {
            this.orderByClause = orderByClause;
            return this;
        }

        public Builder perPartitionLimit(String perPartitionLimitClause) {
            this.perPartitionLimitClause = perPartitionLimitClause;
            return this;
        }

        public Builder limit(String limitClause) {
            this.limitClause = limitClause;
            return this;
        }

        public Builder allowFilter() {
            allowFilter = true;
            return this;
        }

        public PrestoGraphQuery build() { return new PrestoGraphQuery(this);}

    }

}
