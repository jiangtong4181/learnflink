package com;
import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.zhao.canal_demo.CanalModel;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;

/**
 * 1，创建链接
 * 2，建立连接
 * 3，订阅主题
 * 4，获取数据
 * 5，递交确认
 * 6，关闭连接
 */
public class CanalDemo {
    public static void main(String[] args) {
        boolean flag = true;
        int batch = 1000;
        //创建链接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop101", 11111), "example", "", "");
        //CanalConnector canalConnector = CanalConnectors.newClusterConnector("hadoop101:2181,hadoop102:2181,hadoop103:2181", "example", "", "");
        //建立链接
        try {
            canalConnector.connect();
            //设立回滚
            canalConnector.rollback();
            //订阅主题
            canalConnector.subscribe("learn.*");
            //不定的在拉取数据
            while (flag) {
                Message message = canalConnector.getWithoutAck(batch);
                //获取批次id
                long bitchId = message.getId();
                //获取binlog的日志总数
                int size = message.getEntries().size();
                if (size == 0 || size == -1) {

                } else {
                    //byte[] bytes = biglogToProtobuf(message);
                    //写入到kafka即可
                    printSummary(message);
                }
                //递交确认
                canalConnector.ack(bitchId);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //关闭链接
            canalConnector.disconnect();
        }
    }

    private static void printSummary(Message message) {
        //遍历整个bitch中的每一个binlog实体
        List<CanalEntry.Entry> messageEntries = message.getEntries();
        for (CanalEntry.Entry messageEntry : messageEntries) {
            //如果是事务开始和结束,不需要处理，应该处理数据本身
            if (messageEntry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || messageEntry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }
            //获取binglog文件名
            String logfileName = messageEntry.getHeader().getLogfileName();
            //获取logfile的偏移量
            long logfileOffset = messageEntry.getHeader().getLogfileOffset();
            //获取sql执行语句时间戳
            long executeTime = messageEntry.getHeader().getExecuteTime();
            //获取数据库名
            String schemaName = messageEntry.getHeader().getSchemaName();
            //获取表名
            String tableName = messageEntry.getHeader().getTableName();
            //获取事件类型
            String eventType = messageEntry.getHeader().getEventType().toString().toLowerCase();

            System.out.println("logfileName : " + logfileName);
            System.out.println("logfileOffset : " + logfileOffset);
            System.out.println("executeTime : " + executeTime);
            System.out.println("schemaName : " + schemaName);
            System.out.println("tableName : " + tableName);
            System.out.println("eventType : " + eventType);

            CanalEntry.RowChange rowChange = null;

            try {
                rowChange = CanalEntry.RowChange.parseFrom(messageEntry.getStoreValue());
            } catch (Exception e) {
                e.printStackTrace();
            }

            //迭代每一条变更数据
            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                if (messageEntry.getHeader().getEventType() == CanalEntry.EventType.DELETE) {
                    System.out.println("-------delete-------");
                    printColumnList(rowData.getBeforeColumnsList());
                    System.out.println("--------------------");
                } else if (messageEntry.getHeader().getEventType() == CanalEntry.EventType.UPDATE) {
                    System.out.println("-------update-------");
                    printColumnList(rowData.getBeforeColumnsList());
                    System.out.println("--------------------");
                } else if (messageEntry.getHeader().getEventType() == CanalEntry.EventType.INSERT) {
                    System.out.println("-------insert-------");
                    printColumnList(rowData.getBeforeColumnsList());
                    System.out.println("--------------------");
                }
            }
        }
    }

    //tojson
    private static String BinLogToJson(Message message) throws InvalidProtocolBufferException {
        //创建map结构保存最终的数据
        HashMap<String, Object> RowDateMap = new HashMap<String, Object>();
        //遍历整个bitch中的每一个binlog实体
        List<CanalEntry.Entry> messageEntries = message.getEntries();
        for (CanalEntry.Entry messageEntry : messageEntries) {
            //如果是事务开始和结束,不需要处理，应该处理数据本身
            if (messageEntry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || messageEntry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }
            //获取binglog文件名
            String logfileName = messageEntry.getHeader().getLogfileName();
            //获取logfile的偏移量
            long logfileOffset = messageEntry.getHeader().getLogfileOffset();
            //获取sql执行语句时间戳
            long executeTime = messageEntry.getHeader().getExecuteTime();
            //获取数据库名
            String schemaName = messageEntry.getHeader().getSchemaName();
            //获取表名
            String tableName = messageEntry.getHeader().getTableName();
            //获取事件类型
            String eventType = messageEntry.getHeader().getEventType().toString().toLowerCase();

            System.out.println("logfileName : " + logfileName);
            System.out.println("logfileOffset : " + logfileOffset);
            System.out.println("executeTime : " + executeTime);
            System.out.println("schemaName : " + schemaName);
            System.out.println("tableName : " + tableName);
            System.out.println("eventType : " + eventType);

            HashMap<String, Object> columnMap = new HashMap<String, Object>();

            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(messageEntry.getStoreValue());
            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
            for (CanalEntry.RowData rowData : rowDatasList) {
                if (eventType.equals("insert") || eventType.equals("update")) {
                    for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                        columnMap.put(column.getName(), column.getValue());
                    }
                } else if (eventType.equals("delete")) {
                    for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                        columnMap.put(column.getName(), column.getValue());
                    }
                }
            }
            RowDateMap.put("colume",columnMap);
        }
        return JSON.toJSONString(RowDateMap);
    }

    //binlog解析protobuf
    private static byte[] biglogToProtobuf(Message message) throws Exception{
        //构建canalmodel实体
        CanalModel.RowDate.Builder rowDateBuider = CanalModel.RowDate.newBuilder();
        List<CanalEntry.Entry> messageEntries = message.getEntries();
        for (CanalEntry.Entry messageEntry : messageEntries) {
            //如果是事务开始和结束,不需要处理，应该处理数据本身
            if (messageEntry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || messageEntry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }
            //获取binglog文件名
            String logfileName = messageEntry.getHeader().getLogfileName();
            //获取logfile的偏移量
            long logfileOffset = messageEntry.getHeader().getLogfileOffset();
            //获取sql执行语句时间戳
            long executeTime = messageEntry.getHeader().getExecuteTime();
            //获取数据库名
            String schemaName = messageEntry.getHeader().getSchemaName();
            //获取表名
            String tableName = messageEntry.getHeader().getTableName();
            //获取事件类型
            String eventType = messageEntry.getHeader().getEventType().toString().toLowerCase();

            rowDateBuider.setLogfileName(logfileName);
            rowDateBuider.setLogfileOffset(logfileOffset);
            rowDateBuider.setExecuteTime(executeTime);
            rowDateBuider.setSchemaName(schemaName);
            rowDateBuider.setTableName(tableName);
            rowDateBuider.setEventType(eventType);

            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(messageEntry.getStoreValue());
            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
            for (CanalEntry.RowData rowData : rowDatasList) {
                if (eventType.equals("insert") || eventType.equals("update")) {
                    for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                        rowDateBuider.putColumns(column.getName(),column.getValue());
                    }
                } else if (eventType.equals("delete")) {
                    for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                        rowDateBuider.putColumns(column.getName(),column.getValue());
                    }
                }
            }
        }
        return rowDateBuider.build().toByteArray();
    }


    private static void printColumnList(List<CanalEntry.Column> beforeColumnsList) {
        for (CanalEntry.Column column : beforeColumnsList) {
            System.out.println(column.getName() + "\t" + column.getValue());
        }
    }
}
