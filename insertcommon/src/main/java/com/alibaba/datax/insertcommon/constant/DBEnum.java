package com.alibaba.datax.insertcommon.constant;

import com.alibaba.datax.insertcommon.exception.NoDBTypeException;

/**
 * @author zengwei
 * @date 2019-07-24 10:41
 */
public enum DBEnum {
    /**
     * table store 表格存储
     */
    TABLE_STORE,

    /**
     * 分析性数据库mysql版本
     */
    ANALYTIC_DB;

    public static DBEnum getDBEnum(String type) throws NoDBTypeException {
        for (DBEnum temp : DBEnum.values()) {
            if (temp.toString().equals(type)) {
                return temp;
            }
        }
        throw new NoDBTypeException();
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
