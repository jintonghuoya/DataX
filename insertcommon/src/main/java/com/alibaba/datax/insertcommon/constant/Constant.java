package com.alibaba.datax.insertcommon.constant;

/**
 * @author zengwei
 * @date 2019-07-24 19:41
 */
public class Constant {

    /*
     * table store 相关
     * -----------------------------------------------------------------
     */

    /**
     * 主键列中的逻辑表名
     */
    public static final String TABLE_LOGICAL_NAME = "table_logical_name";

    /**
     * 主键列中的逻辑数据中主键的列的连接字符串
     * 由table_logical_name 和 primary_key_combo 就可保持记录的唯一性
     */
    public static final String PRIMARY_KEY_COMBO = "primary_key_combo";

    /**
     * 主键列中字段，该字段在tableStore保持自增
     */
    public static final String SERIAL_NUMBER = "serial_number";

    /**
     * 主键列中分区键, 由各字段计算出来,保持hash_key的值分布均匀
     */
    public static final String HASH_KEY = "hash_key";

    /**
     * 固定的索引名
     */
    public static final String INDEX_NAME = "pooled_table_main";
}
