package com.alibaba.datax.plugin.reader.tablestorereader.model;

public class TableStoreConst {
    // Reader support type
    public final static String TYPE_STRING  = "STRING";
    public final static String TYPE_INTEGER = "INT";
    public final static String TYPE_DOUBLE  = "DOUBLE";
    public final static String TYPE_BOOLEAN = "BOOL";
    public final static String TYPE_BINARY  = "BINARY";
    public final static String TYPE_INF_MIN = "INF_MIN";
    public final static String TYPE_INF_MAX = "INF_MAX";
    
    // Column
    public final static String NAME = "name";
    public final static String TYPE = "type";
    public final static String VALUE = "value";
    /**
     * 固定以该字段为索引
     */
    public final static String HASH_KEY = "hash_key";

    public final static String OTS_CONF = "OTS_CONF";
    public final static String OTS_RANGE = "OTS_RANGE";
    public final static String OTS_DIRECTION = "OTS_DIRECTION";
    public final static String OTS_HASH_KEY_PREFIX = "OTS_HASH_KEY_PREFIX";

    // options
    public final static String RETRY = "maxRetryTime";
    public final static String SLEEP_IN_MILLI_SECOND = "retrySleepInMillionSecond";

}
