package com.alibaba.datax.plugin.writer.tablestorewriter.model;

import com.alicloud.openservices.tablestore.model.ColumnType;

public class TableStoreAttrColumn implements Comparable<TableStoreAttrColumn> {
    private String name;
    private ColumnType type;
    private int sequence;
    private Boolean primaryKey;

    public TableStoreAttrColumn() {
    }

    public TableStoreAttrColumn(String name, ColumnType type, int sequence, Boolean primaryKey) {
        this.name = name;
        this.type = type;
        this.sequence = sequence;
        this.primaryKey = primaryKey;
    }

    public String getName() {
        return name;
    }

    public ColumnType getType() {
        return type;
    }

    public int getSequence() {
        return sequence;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setType(ColumnType type) {
        this.type = type;
    }

    public void setSequence(int sequence) {
        this.sequence = sequence;
    }

    public Boolean getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(Boolean primaryKey) {
        this.primaryKey = primaryKey;
    }

    @Override
    public int compareTo(TableStoreAttrColumn o) {
        return getSequence() - o.getSequence();
    }
}
