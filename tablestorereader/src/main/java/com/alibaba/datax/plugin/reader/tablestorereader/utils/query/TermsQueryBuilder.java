package com.alibaba.datax.plugin.reader.tablestorereader.utils.query;

import com.alibaba.datax.common.util.Configuration;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.query.TermsQuery;

import java.util.ArrayList;
import java.util.List;

public class TermsQueryBuilder implements QueryBuilder<TermsQuery> {

    private static String TERMS = "terms";
    private static String TERMS_VALUE = "value";
    private static String TERMS_TYPE = "type";
    private static String FIELDNAME = "fieldName";

    @Override
    public TermsQuery build(Configuration configuration) throws Exception {
        TermsQuery termsQuery = new TermsQuery();

        List<ColumnValue> terms = new ArrayList<>();

        List<Configuration> listConfiguration = configuration.getListConfiguration(TERMS);

        for (Configuration configuration1 : listConfiguration) {

            Object o = configuration1.get(TERMS_VALUE);
            ColumnType termColumnType = TableStoreDataTypeUtil.type(configuration1.getString(TERMS_TYPE));

            if (null == termColumnType) {
                throw new NoColumnTypeFoundException();
            }

            terms.add(new ColumnValue(o, termColumnType));
        }

        termsQuery.setTerms(terms);
        termsQuery.setFieldName(configuration.getString(FIELDNAME));

        return termsQuery;
    }

    public static void main(String[] args) throws Exception {

        TermsQueryBuilder termsQueryBuilder = new TermsQueryBuilder();

        String json = "{\"fieldName\":\"ok\", \"terms\":[{\"value\":\"okko\",\"type\":\"STRING\"}]}";
        TermsQuery build = termsQueryBuilder.build(Configuration.from(json));

        System.out.println(build);
    }
}
