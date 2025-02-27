package com.alibaba.datax.plugin.reader.tablestorereader.utils.query;

import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.search.query.QueryOperator;
import com.alicloud.openservices.tablestore.model.search.query.ScoreMode;

public class TableStoreDataTypeUtil {

    /**
     * 获取字段的ColumnType枚举类型
     *
     * @param a rpc返回为Object的数据(在网络的序列化中均为STRING)
     * @return ColumnType枚举
     */
    public static ColumnType type(String a) {
        for (ColumnType c : ColumnType.values()) {
            if (a.equals(c.toString())) {
                return c;
            }
        }

        return null;
    }


    /**
     * 获取字段的QueryOperator枚举类型
     *
     * @param a rpc返回为Object的数据(在网络的序列化中均为STRING)
     * @return QueryOperator枚举
     */
    public static QueryOperator queryOperatorType(String a) {
        for (QueryOperator c : QueryOperator.values()) {
            if (a.equals(c.toString())) {
                return c;
            }
        }

        return null;
    }

    /**
     * 获取字段的ScoreMode枚举类型
     *
     * @param a rpc返回为Object的数据(在网络的序列化中均为STRING)
     * @return ScoreMode枚举
     */
    public static ScoreMode scoreModeType(String a) {
        for (ScoreMode c : ScoreMode.values()) {
            if (a.equals(c.toString())) {
                return c;
            }
        }

        return null;
    }
}