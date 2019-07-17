package com.alibaba.datax.plugin.reader.tablestorereader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.tablestorereader.model.TableStoreConf;
import com.alibaba.datax.plugin.reader.tablestorereader.model.TableStoreConst;
import com.alibaba.datax.plugin.reader.tablestorereader.utils.Common;
import com.alibaba.datax.plugin.reader.tablestorereader.utils.GsonParser;
import com.alibaba.datax.plugin.reader.tablestorereader.utils.query.QueryFactory;
import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.search.SearchQuery;
import com.alicloud.openservices.tablestore.model.search.SearchRequest;
import com.alicloud.openservices.tablestore.model.search.SearchResponse;
import com.alicloud.openservices.tablestore.model.search.query.BoolQuery;
import com.alicloud.openservices.tablestore.model.search.query.PrefixQuery;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.google.common.collect.Lists;
import com.meizhilab.basis.system.util.kryo.KryoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

public class TableStoreReaderSlaveProxy {


    private static final Logger LOG = LoggerFactory.getLogger(TableStoreReaderSlaveProxy.class);

    private void rowsToSender(List<Row> rows, RecordSender sender, List<String> columns) {
        for (Row row : rows) {
            Record line = sender.createRecord();
            line = Common.parseRowToLine(row, columns, line);

            LOG.debug("Reader send record : {}", line.toString());

            sender.sendToWriter(line);
        }
    }

    public void read(RecordSender sender, Configuration configuration) throws Exception {

        TableStoreConf conf = GsonParser.jsonToConf(configuration.getString(TableStoreConst.OTS_CONF));
        String hashKeyPrefix = configuration.getString(TableStoreConst.OTS_HASH_KEY_PREFIX);

        Object o = KryoUtil.readFromString(conf.getQueryRow());

        BoolQuery boolQuery = new BoolQuery();
        PrefixQuery prefixQuery = new PrefixQuery();
        prefixQuery.setPrefix(hashKeyPrefix);
        prefixQuery.setFieldName(TableStoreConst.HASH_KEY);

        boolQuery.setMustQueries(Lists.newArrayList(prefixQuery, (Query) o));

        ClientConfiguration configure1 = new ClientConfiguration();

        SyncClient syncClient = new SyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccesskey(),
                conf.getInstanceName(),
                configure1,
                null,
                null
        );

        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setQuery(boolQuery);
        searchQuery.setLimit(conf.getLimit());
        searchQuery.setGetTotalCount(true);
        SearchRequest searchRequest = new SearchRequest(conf.getTableName(), conf.getIndexName(), searchQuery);
        SearchRequest.ColumnsToGet columnsToGet = new SearchRequest.ColumnsToGet();
        columnsToGet.setColumns(conf.getColumnNames());
        searchRequest.setColumnsToGet(columnsToGet);

        int count = 0;

        SearchResponse resp = syncClient.search(searchRequest);

        if (!resp.isAllSuccess()) {
            throw new RuntimeException("not all success");
        }

        count += resp.getRows().size();
        rowsToSender(resp.getRows(), sender, conf.getColumnNames());

        while (resp.getNextToken() != null) {
            //把Token设置到下一次请求中
            searchRequest.getSearchQuery().setToken(resp.getNextToken());
            resp = syncClient.search(searchRequest);

            if (!resp.isAllSuccess()) {
                throw new RuntimeException("not all success");
            }

            count += resp.getRows().size();
            rowsToSender(resp.getRows(), sender, conf.getColumnNames());
        }

        syncClient.shutdown();

        LOG.info("get by token totalCount:{}", resp.getTotalCount());
        LOG.info("read end. hash_key:{} count:{}", hashKeyPrefix, count);
    }
}
