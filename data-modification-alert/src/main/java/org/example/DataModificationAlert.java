package org.example;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;

import java.io.IOException;
import java.util.HashMap;

/**
 * Data Modification Alert
 *
 */
public class DataModificationAlert
{
    public static void main(String[] args) {
        BinaryLogClient client = new BinaryLogClient("192.168.1.205", 3306, "root", "root");
        client.setServerId(123456);

        HashMap<Long, String> tableMap = new HashMap<Long, String>();
        client.registerEventListener(event -> {
            // EventData data = event.getData();
            System.out.println("---> " + event.toString());
            EventType eventType = event.getHeader().getEventType();
            if (eventType == EventType.TABLE_MAP) {
                TableMapEventData data = (TableMapEventData) event.getData();
                tableMap.put(data.getTableId(), data.getTable());
                System.out.println(tableMap);
                // System.out.println(tableMap.getOrDefault(Long.valueOf(95), "123"));

            } else if (eventType == EventType.EXT_UPDATE_ROWS) {
                UpdateRowsEventData data = (UpdateRowsEventData) event.getData();
                if (tableMap.getOrDefault(Long.valueOf(data.getTableId()), "").equals("transfer_logs")) {
                    // 发送邮件
                    System.out.println("发送邮件：transfer_logs 有人恶意修改转账日志，请及时核查");
                }
            } else if (eventType == EventType.EXT_DELETE_ROWS) {
                DeleteRowsEventData data = (DeleteRowsEventData) event.getData();
                if (tableMap.getOrDefault(Long.valueOf(data.getTableId()), "").equals("transfer_logs")) {
                    // 发送邮件
                    System.out.println("发送邮件：transfer_logs 有人恶意删除转账日志，请及时核查");
                }
            }
            // System.out.println("---> " + event.getData().toString());
            // if (data instanceof TableMapEventData) {
            //     System.out.println("Table:");
            //     TableMapEventData tableMapEventData = (TableMapEventData) data;
            //     System.out.println(tableMapEventData.getTableId()+": ["+tableMapEventData.getDatabase() + "-" + tableMapEventData.getTable()+"]");
            // }
            // if (data instanceof UpdateRowsEventData) {
            //     System.out.println("Update:");
            //     System.out.println(data.toString());
            // } else if (data instanceof WriteRowsEventData) {
            //     System.out.println("Insert:");
            //     System.out.println(data.toString());
            // } else if (data instanceof DeleteRowsEventData) {
            //     System.out.println("Delete:");
            //     System.out.println(data.toString());
            // }
        });

        try {
            client.connect();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
