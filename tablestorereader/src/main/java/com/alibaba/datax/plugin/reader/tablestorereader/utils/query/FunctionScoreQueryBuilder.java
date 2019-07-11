package com.alibaba.datax.plugin.reader.tablestorereader.utils.query;

import com.alibaba.datax.common.util.Configuration;
import com.alicloud.openservices.tablestore.model.search.query.FieldValueFactor;
import com.alicloud.openservices.tablestore.model.search.query.FunctionScoreQuery;
import com.alicloud.openservices.tablestore.model.search.query.Query;

public class FunctionScoreQueryBuilder implements QueryBuilder<FunctionScoreQuery> {

    private static String query = "query";
    private static String fieldValueFactor = "fieldValueFactor";
    private static String fieldValueFactor_fieldName = "fieldName";

    @Override
    public FunctionScoreQuery build(Configuration configuration) throws Exception {

        Configuration configuration1 = configuration.getConfiguration(query);

        Query build = QueryFactory.build(configuration1, true);

        Configuration configuration2 = configuration.getConfiguration(fieldValueFactor);

        FieldValueFactor fieldValueFactor = new FieldValueFactor(null);

        if (null != configuration2) {
            String string = configuration2.getString(fieldValueFactor_fieldName);
            fieldValueFactor.setFieldName(string);
        }

        FunctionScoreQuery functionScoreQuery = new FunctionScoreQuery(build, fieldValueFactor);
        functionScoreQuery.setQuery(QueryFactory.build(configuration1, true));

        return functionScoreQuery;
    }
}

