package com.alibaba.datax.plugin.reader.tablestorereader.utils.query;

import com.alibaba.datax.common.util.Configuration;
import com.alicloud.openservices.tablestore.model.search.query.BoolQuery;
import com.alicloud.openservices.tablestore.model.search.query.Query;

import java.util.ArrayList;
import java.util.List;

public class BoolQueryBuilder implements QueryBuilder<BoolQuery> {

    private static String mustQueries = "mustQueries";
    private static String mustNotQueries = "mustNotQueries";
    private static String filterQueries = "filterQueries";
    private static String shouldQueries = "shouldQueries";
    private static String minimumShouldMatch = "minimumShouldMatch";

    @Override
    public BoolQuery build(Configuration configuration) throws Exception {

        BoolQuery boolQuery = new BoolQuery();

        List<Configuration> listConfiguration = configuration.getListConfiguration(mustQueries);

        List<Query> objects = new ArrayList<>();

        if (null != listConfiguration) {

            for (Configuration configuration1 : listConfiguration) {
                objects.add(QueryFactory.build(configuration1, true));
            }

            boolQuery.setMustQueries(objects);
        }

        List<Configuration> listConfiguration2 = configuration.getListConfiguration(mustNotQueries);

        List<Query> object2 = new ArrayList<>();

        if (null != listConfiguration2) {

            for (Configuration configuration1 : listConfiguration2) {
                object2.add(QueryFactory.build(configuration1, true));
            }

            boolQuery.setMustNotQueries(object2);
        }

        List<Configuration> listConfiguration3 = configuration.getListConfiguration(filterQueries);

        List<Query> object3 = new ArrayList<>();

        if (null != listConfiguration3) {

            for (Configuration configuration1 : listConfiguration3) {
                object3.add(QueryFactory.build(configuration1, true));
            }

            boolQuery.setFilterQueries(object3);
        }

        List<Configuration> listConfiguration4 = configuration.getListConfiguration(shouldQueries);

        List<Query> object4 = new ArrayList<>();

        if (null != listConfiguration4) {

            for (Configuration configuration1 : listConfiguration4) {
                object4.add(QueryFactory.build(configuration1, true));
            }

            boolQuery.setShouldQueries(object4);
        }

        int anInt = configuration.getInt(minimumShouldMatch, 0);

        boolQuery.setMinimumShouldMatch(anInt);

        return boolQuery;
    }
}
