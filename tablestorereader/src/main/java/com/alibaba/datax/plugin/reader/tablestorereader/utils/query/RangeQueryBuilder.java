package com.alibaba.datax.plugin.reader.tablestorereader.utils.query;

import com.alibaba.datax.common.util.Configuration;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.query.RangeQuery;

public class RangeQueryBuilder implements QueryBuilder<RangeQuery> {
    private static String fieldName = "fieldName";
    private static String from = "from";
    private static String includeLower = "includeLower";
    private static String includeUpper = "includeUpper";

    private static String to = "to";
    private static String value = "value";
    private static String type = "type";


    @Override
    public RangeQuery build(Configuration configuration) throws Exception {

        RangeQuery prefixQuery = new RangeQuery();

        prefixQuery.setFieldName(configuration.getString(fieldName));

        Configuration configurationForm = configuration.getConfiguration(from);

        ColumnType fromColumnType = TableStoreDataTypeUtil.type(configurationForm.getString(type));
        Object from = configurationForm.get(value);

        if (null == fromColumnType) {
            throw new NoColumnTypeFoundException();
        }

        prefixQuery.setFrom(new ColumnValue(from, fromColumnType));

        Configuration configurationTo = configuration.getConfiguration(to);

        ColumnType toColumnType = TableStoreDataTypeUtil.type(configurationTo.getString(type));
        Object to = configurationTo.get(value);

        if (null == toColumnType) {
            throw new NoColumnTypeFoundException();
        }

        prefixQuery.setTo(new ColumnValue(to, toColumnType));

        prefixQuery.setIncludeLower(configuration.getBool(includeLower));
        prefixQuery.setIncludeUpper(configuration.getBool(includeUpper));

        return prefixQuery;
    }
}
