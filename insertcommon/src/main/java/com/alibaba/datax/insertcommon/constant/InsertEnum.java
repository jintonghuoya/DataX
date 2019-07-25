package com.alibaba.datax.insertcommon.constant;

import com.alibaba.datax.insertcommon.exception.NoInsertTypeException;

/**
 * 业务定制insert类型
 * 不同insert类型，导入逻辑不同
 *
 * @author zengwei
 * @date 2019-07-24 10:21
 */
public enum InsertEnum {

    TWM01("仅插入新条目, 忽略重复条目"),
    TWM02("插入新条目, 并更新重复条目(中的指定字段)"),
    TWM04("清空目标表, 并插入新条目"),
    TWM05("删除重复条目, 插入新条目");

    private String desc;

    InsertEnum(String desc) {
        this.desc = desc;
    }

    public String getDesc() {
        return desc;
    }

    public static InsertEnum getInsertEnum(String type) throws NoInsertTypeException {
        for (InsertEnum temp : InsertEnum.values()) {
            if (temp.toString().equals(type)) {
                return temp;
            }
        }
        throw new NoInsertTypeException();
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
