package com.alibaba.datax.plugin.writer.tablestorewriter.utils;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.tablestorewriter.model.TableStoreAttrColumn;
import com.alibaba.datax.plugin.writer.tablestorewriter.model.TableStoreErrorMessage;
import com.alibaba.datax.plugin.writer.tablestorewriter.model.TableStorePKColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.TableMeta;

import java.util.*;
import java.util.Map.Entry;

public class ParamChecker {

    private static void throwNotExistException(String key) {
        throw new IllegalArgumentException(String.format(TableStoreErrorMessage.MISSING_PARAMTER_ERROR, key));
    }

    private static void throwStringLengthZeroException(String key) {
        throw new IllegalArgumentException(String.format(TableStoreErrorMessage.PARAMTER_STRING_IS_EMPTY_ERROR, key));
    }

    private static void throwEmptyListException(String key) {
        throw new IllegalArgumentException(String.format(TableStoreErrorMessage.PARAMETER_LIST_IS_EMPTY_ERROR, key));
    }

    private static void throwNotListException(String key) {
        throw new IllegalArgumentException(String.format(TableStoreErrorMessage.PARAMETER_IS_NOT_ARRAY_ERROR, key));
    }

    private static void throwNotMapException(String key) {
        throw new IllegalArgumentException(String.format(TableStoreErrorMessage.PARAMETER_IS_NOT_MAP_ERROR, key));
    }

    public static String checkStringAndGet(Configuration param, String key) {
        String value = param.getString(key);
        if (null == value) {
            throwNotExistException(key);
        } else if (value.length() == 0) {
            throwStringLengthZeroException(key);
        }
        return value;
    }

    public static List<Object> checkListAndGet(Configuration param, String key, boolean isCheckEmpty) {
        List<Object> value = null;
        try {
            value = param.getList(key);
        } catch (ClassCastException e) {
            throwNotListException(key);
        }
        if (null == value) {
            throwNotExistException(key);
        } else if (isCheckEmpty && value.isEmpty()) {
            throwEmptyListException(key);
        }
        return value;
    }

    public static List<Object> checkListAndGet(Map<String, Object> range, String key) {
        Object obj =  range.get(key);
        if (null == obj) {
            return null;
        }
        return checkListAndGet(range, key, false);
    }

    public static List<Object> checkListAndGet(Map<String, Object> range, String key, boolean isCheckEmpty) {
        Object obj =  range.get(key);
        if (null == obj) {
            throwNotExistException(key);
        }
        if (obj instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> value = (List<Object>)obj;
            if (isCheckEmpty && value.isEmpty()) {
                throwEmptyListException(key);
            }
            return value;
        } else {
            throw new IllegalArgumentException(String.format(TableStoreErrorMessage.PARSE_TO_LIST_ERROR, key));
        }
    }

    public static List<Object> checkListAndGet(Map<String, Object> range, String key, List<Object> defaultList) {
        Object obj =  range.get(key);
        if (null == obj) {
            return defaultList;
        }
        if (obj instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> value = (List<Object>)obj;
            return value;
        } else {
            throw new IllegalArgumentException(String.format(TableStoreErrorMessage.PARSE_TO_LIST_ERROR, key));
        }
    }

    public static Map<String, Object> checkMapAndGet(Configuration param, String key, boolean isCheckEmpty) {
        Map<String, Object> value = null;
        try {
            value = param.getMap(key);
        } catch (ClassCastException e) {
            throwNotMapException(key);
        }
        if (null == value) {
            throwNotExistException(key);
        } else if (isCheckEmpty && value.isEmpty()) {
            throwEmptyListException(key);
        }
        return value;
    }
    
    public static void checkPrimaryKey(TableMeta meta, List<TableStorePKColumn> pk) {
        Map<String, PrimaryKeyType> types = meta.getPrimaryKeyMap();
        // 个数是否相等
        if (types.size() != pk.size()) {
            throw new IllegalArgumentException(String.format(TableStoreErrorMessage.INPUT_PK_COUNT_NOT_EQUAL_META_ERROR, pk.size(), types.size()));
        }
        
        // 名字类型是否相等
        Map<String, PrimaryKeyType> inputTypes = new HashMap<String, PrimaryKeyType>();
        for (TableStorePKColumn col : pk) {
            inputTypes.put(col.getName(), col.getType());
        }
        
        for (Entry<String, PrimaryKeyType> e : types.entrySet()) {
            if (!inputTypes.containsKey(e.getKey())) {
                throw new IllegalArgumentException(String.format(TableStoreErrorMessage.PK_COLUMN_MISSING_ERROR, e.getKey()));
            }
            PrimaryKeyType type = inputTypes.get(e.getKey());
            if (type != e.getValue()) {
                throw new IllegalArgumentException(String.format(TableStoreErrorMessage.INPUT_PK_TYPE_NOT_MATCH_META_ERROR, e.getKey(), type, e.getValue()));
            }
        }
    }
    
    public static void checkAttribute(List<TableStoreAttrColumn> attr) {
        // 检查重复列
        Set<String> names = new HashSet<String>();
        for (TableStoreAttrColumn col : attr) {
            if (names.contains(col.getName())) {
                throw new IllegalArgumentException(String.format(TableStoreErrorMessage.ATTR_REPEAT_COLUMN_ERROR, col.getName()));
            } else {
                names.add(col.getName());
            }
        }
    }
}
