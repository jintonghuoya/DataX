package com.alibaba.datax.plugin.writer.tablestorewriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.insertcommon.constant.Constant;
import com.alibaba.datax.insertcommon.constant.InsertEnum;
import com.alibaba.datax.plugin.writer.tablestorewriter.utils.Common;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.DefaultTableStoreWriter;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.writer.WriterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

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
                LOG.error("OTSException: {}",  e.getMessage(), e);
                throw DataXException.asDataXException(new TableStoreWriterError(e.getErrorCode(), "OTS端的错误"), Common.getDetailMessage(e), e);
            } catch (ClientException e) {
                LOG.error("ClientException: {}",  e.getMessage(), e);
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
            if (this.proxy.getTableStoreConfig().getInsertMode().equals(InsertEnum.TWM04)) {

                WriterConfig writerConfig = new WriterConfig();
                writerConfig.setConcurrency(this.proxy.getTableStoreConfig().getConcurrencyWrite());
                writerConfig.setMaxBatchRowsCount(this.proxy.getTableStoreConfig().getBatchWriteCount());
                writerConfig.setMaxBatchSize(this.proxy.getTableStoreConfig().getRestrictConfig().getRequestTotalSizeLimitation());
                writerConfig.setBufferSize(this.proxy.getTableStoreConfig().getBufferSize());
                writerConfig.setMaxAttrColumnSize(this.proxy.getTableStoreConfig().getRestrictConfig().getAttributeColumnSize());
                writerConfig.setMaxColumnsCount(this.proxy.getTableStoreConfig().getRestrictConfig().getMaxColumnsCount());
                writerConfig.setMaxPKColumnSize(this.proxy.getTableStoreConfig().getRestrictConfig().getPrimaryKeyColumnSize());

                SyncClient syncClient = new SyncClient(
                        this.proxy.getTableStoreConfig().getEndpoint(),
                        this.proxy.getTableStoreConfig().getAccessId(),
                        this.proxy.getTableStoreConfig().getAccessKey(),
                        this.proxy.getTableStoreConfig().getInstanceName()
                );

                DeleteTableRequest request = new DeleteTableRequest(this.proxy.getTableStoreConfig().getTableName());

                syncClient.deleteTable(request);

                TableMeta tableMeta = new TableMeta(this.proxy.getTableStoreConfig().getTableName());
                tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(Constant.TABLE_LOGICAL_NAME, PrimaryKeyType.STRING));
                tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(Constant.PRIMARY_KEY_COMBO, PrimaryKeyType.INTEGER));
                tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(Constant.SERIAL_NUMBER, PrimaryKeyType.STRING));
                tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(Constant.HASH_KEY, PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT));

                int timeToLive = -1; // 数据的过期时间, 单位秒, -1代表永不过期. 带索引表的主表数据过期时间必须为-1。
                int maxVersions = 1; // 保存的最大版本数, 带索引表的请表最大版本数必须为1。

                TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);

                ArrayList<IndexMeta> indexMetas = new ArrayList<IndexMeta>();
                IndexMeta indexMeta = new IndexMeta(Constant.INDEX_NAME);

                indexMeta.addPrimaryKeyColumn(Constant.TABLE_LOGICAL_NAME); // 为索引表添加主键列。
                indexMeta.addPrimaryKeyColumn(Constant.HASH_KEY); // 为索引表添加主键列。
                indexMeta.addPrimaryKeyColumn(Constant.PRIMARY_KEY_COMBO); // 为索引表添加主键列。
                indexMetas.add(indexMeta);

                CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions, indexMetas);// 创建主表时一同创建索引表。

                syncClient.createTable(createTableRequest);
            }
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private TableStoreWriterSlaveProxy proxy = new TableStoreWriterSlaveProxy();

        @Override
        public void init() {}

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
                LOG.error("OTSException: {}",  e.getMessage(), e);
                throw DataXException.asDataXException(new TableStoreWriterError(e.getErrorCode(), "OTS端的错误"), Common.getDetailMessage(e), e);
            } catch (ClientException e) {
                LOG.error("ClientException: {}",  e.getMessage(), e);
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
