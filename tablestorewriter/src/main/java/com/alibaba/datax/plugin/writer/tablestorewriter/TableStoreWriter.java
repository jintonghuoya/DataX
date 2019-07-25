package com.alibaba.datax.plugin.writer.tablestorewriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.insertcommon.constant.Constant;
import com.alibaba.datax.insertcommon.constant.InsertEnum;
import com.alibaba.datax.plugin.writer.tablestorewriter.utils.Common;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.search.*;
import com.alicloud.openservices.tablestore.model.search.query.TermQuery;
import com.alicloud.openservices.tablestore.model.search.sort.PrimaryKeySort;
import com.alicloud.openservices.tablestore.model.search.sort.Sort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TableStoreWriter {
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private TableStoreWriterMasterProxy proxy = new TableStoreWriterMasterProxy();

        @Override
        public void init() {
            LOG.info("init() begin ...");
            try {
                this.proxy.init(getPluginJobConf());
            } catch (TableStoreException e) {
                LOG.error("OTSException: {}", e.getMessage(), e);
                throw DataXException.asDataXException(new TableStoreWriterError(e.getErrorCode(), "OTS端的错误"), Common.getDetailMessage(e), e);
            } catch (ClientException e) {
                LOG.error("ClientException: {}", e.getMessage(), e);
                throw DataXException.asDataXException(new TableStoreWriterError(e.getTraceId(), "OTS端的错误"), Common.getDetailMessage(e), e);
            } catch (IllegalArgumentException e) {
                LOG.error("IllegalArgumentException. ErrorMsg:{}", e.getMessage(), e);
                throw DataXException.asDataXException(TableStoreWriterError.INVALID_PARAM, Common.getDetailMessage(e), e);
            } catch (Exception e) {
                LOG.error("Exception. ErrorMsg:{}", e.getMessage(), e);
                throw DataXException.asDataXException(TableStoreWriterError.ERROR, Common.getDetailMessage(e), e);
            }
            LOG.info("init() end ...");
        }

        @Override
        public void destroy() {
            this.proxy.close();
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            try {
                return this.proxy.split(mandatoryNumber);
            } catch (Exception e) {
                LOG.error("Exception. ErrorMsg:{}", e.getMessage(), e);
                throw DataXException.asDataXException(TableStoreWriterError.ERROR, Common.getDetailMessage(e), e);
            }
        }

        @Override
        public void prepare() {
            if (this.proxy.getTableStoreConfig().getInsertMode().equals(InsertEnum.TWM04.toString())) {
                LOG.info("delete all logic table:{} records", this.proxy.getTableStoreConfig().getTableLogicalName());
                deleteAllRecords();
            }
        }

        /**
         * 使用Token进行翻页，这个例子会把所有数据读出，放到一个List中。
         */
        private void deleteAllRecords() {
            SearchQuery searchQuery = new SearchQuery();
            TermQuery termQuery = new TermQuery();
            termQuery.setFieldName(Constant.TABLE_LOGICAL_NAME);
            termQuery.setTerm(new ColumnValue(this.proxy.getTableStoreConfig().getTableLogicalName(), ColumnType.STRING));
            searchQuery.setQuery(termQuery);
            searchQuery.setGetTotalCount(true);
            searchQuery.setLimit(100);
            SearchRequest searchRequest = new SearchRequest(this.proxy.getTableStoreConfig().getTableName(), Constant.INDEX_NAME, searchQuery);

            SearchResponse resp = this.proxy.getSyncClient().search(searchRequest);

            if (!resp.isAllSuccess()) {
                throw new RuntimeException("not all success");
            }

            if (resp.getRows() != null && resp.getRows().size() > 0) {
                batchWriteRow(resp.getRows());
            }

            LOG.info("all size:{} will be deleted", resp.getTotalCount());

            while (resp.getNextToken() != null) { //读到NextToken为null为止，即读出全部数据
                //把Token设置到下一次请求中
                searchRequest.getSearchQuery().setToken(resp.getNextToken());
                resp = this.proxy.getSyncClient().search(searchRequest);

                if (!resp.isAllSuccess()) {
                    throw new RuntimeException("not all success");
                }

                if (resp.getRows() != null && resp.getRows().size() > 0) {
                    batchWriteRow(resp.getRows());
                }
            }
        }

        private void batchWriteRow(List<Row> rows) {

            BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();

            for (Row row : rows) {
                RowDeleteChange rowDeleteChange = new RowDeleteChange(this.proxy.getTableStoreConfig().getTableName(), row.getPrimaryKey());

                batchWriteRowRequest.addRowChange(rowDeleteChange);
            }

            BatchWriteRowResponse response = this.proxy.getSyncClient().batchWriteRow(batchWriteRowRequest);

            LOG.info("是否全部成功:" + response.isAllSucceed());

            if (!response.isAllSucceed()) {
                for (BatchWriteRowResponse.RowResult rowResult : response.getFailedRows()) {
                    LOG.error("失败的行:" + batchWriteRowRequest.getRowChange(rowResult.getTableName(), rowResult.getIndex()).getPrimaryKey());
                    LOG.error("失败原因:" + rowResult.getError());
                }
                /**
                 * 可以通过createRequestForRetry方法再构造一个请求对失败的行进行重试.这里只给出构造重试请求的部分.
                 * 推荐的重试方法是使用SDK的自定义重试策略功能, 支持对batch操作的部分行错误进行重试. 设定重试策略后, 调用接口处即不需要增加重试代码.
                 */
//                BatchWriteRowRequest retryRequest = batchWriteRowRequest.createRequestForRetry(response.getFailedRows());


            }
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private TableStoreWriterSlaveProxy proxy = new TableStoreWriterSlaveProxy();

        @Override
        public void init() {
        }

        @Override
        public void destroy() {
            this.proxy.close();
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            LOG.info("startWrite() begin ...");
            try {
                this.proxy.init(this.getPluginJobConf());
                this.proxy.write(lineReceiver, this.getTaskPluginCollector());
            } catch (TableStoreException e) {
                LOG.error("OTSException: {}", e.getMessage(), e);
                throw DataXException.asDataXException(new TableStoreWriterError(e.getErrorCode(), "OTS端的错误"), Common.getDetailMessage(e), e);
            } catch (ClientException e) {
                LOG.error("ClientException: {}", e.getMessage(), e);
                throw DataXException.asDataXException(new TableStoreWriterError(e.getTraceId(), "OTS端的错误"), Common.getDetailMessage(e), e);
            } catch (IllegalArgumentException e) {
                LOG.error("IllegalArgumentException. ErrorMsg:{}", e.getMessage(), e);
                throw DataXException.asDataXException(TableStoreWriterError.INVALID_PARAM, Common.getDetailMessage(e), e);
            } catch (Exception e) {
                LOG.error("Exception. ErrorMsg:{}", e.getMessage(), e);
                throw DataXException.asDataXException(TableStoreWriterError.ERROR, Common.getDetailMessage(e), e);
            }
            LOG.info("startWrite() end ...");
        }
    }
}
