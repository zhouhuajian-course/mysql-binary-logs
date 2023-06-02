package org.example;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import redis.clients.jedis.Jedis;

import java.io.Serializable;
import java.sql.*;
import java.util.HashMap;
import java.util.List;

/**
 * 论坛公告
 * 1. 处理插入事件，把职位页面插入redis
 * mysql> create table notifications (id int auto_increment, content text, primary key (id));
 * <p>
 * Event{header=EventHeaderV4{timestamp=0, eventType=ROTATE, serverId=1, headerLength=19, dataLength=25, nextPosition=0, flags=32}, data=RotateEventData{binlogFilename='binlog.000020', binlogPosition=402}}
 * Event{header=EventHeaderV4{timestamp=1685688305000, eventType=FORMAT_DESCRIPTION, serverId=1, headerLength=19, dataLength=103, nextPosition=0, flags=0}, data=FormatDescriptionEventData{binlogVersion=4, serverVersion='8.0.33', headerLength=19, dataLength=98, checksumType=CRC32}}
 * <p>
 * mysql> insert into notifications values (0, '本社区全新改版，...');
 * Query OK, 1 row affected (0.00 sec)
 */
public class DataCacheDemo {
    public static void main(String[] args) throws Exception {

        BinaryLogClient client = new BinaryLogClient("192.168.1.205", 3306, "root", "root");

        Connection conn = DriverManager.getConnection(
            "jdbc:mysql://192.168.1.205:3306/",
            "root",
            "root"
        );
        DatabaseMetaData dbmd = conn.getMetaData();
        HashMap<String, HashMap<Integer, String>> tableColums = new HashMap<>();
        // 遍历数据库
        ResultSet catalogs = dbmd.getCatalogs();
        while (catalogs.next()) {
            String databaseName = catalogs.getString("TABLE_CAT");
            System.out.println(databaseName);
            // 遍历数据库里面的普通表
            ResultSet tables = dbmd.getTables(databaseName, null, null, new String[]{"TABLE"});
            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");
                System.out.println(tableName);
                ResultSet columns = dbmd.getColumns(databaseName, null, tableName, null);
                String key = databaseName + "." + tableName;
                tableColums.put(key, new HashMap<>());
                while (columns.next()) {
                    int ordinalPosition = columns.getInt("ORDINAL_POSITION");
                    String columnName = columns.getString("COLUMN_NAME");
                    tableColums.get(key).put(ordinalPosition, columnName);
                }
            }
        }
        System.out.println(tableColums);

        HashMap<Long, String> tables = new HashMap<>();

        // redis
        Jedis jedis = new Jedis("192.168.1.205", 6379);
// // 尝试操作Redis
//         jedis.set("msg", "Hello World!");
//         String msg = jedis.get("msg");
//         System.out.println(msg);    // 打印"Hello World!"
// // 关闭Redis连接
//         jedis.close();

        client.registerEventListener(event -> {
            System.out.println(event);
            EventType eventType = event.getHeader().getEventType();

            if (eventType == EventType.TABLE_MAP) {
                // Event{header=EventHeaderV4{timestamp=1685696419000, eventType=TABLE_MAP, serverId=1, headerLength=19, dataLength=40, nextPosition=5281, flags=0}, data=TableMapEventData{tableId=98, database='db1', table='users', columnTypes=3, 15, 1, columnMetadata=0, 512, 0, columnNullability={1, 2}, eventMetadata=TableMapEventMetadata{signedness={1}, defaultCharset=255, charsetCollations=null, columnCharsets=null, columnNames=null, setStrValues=null, enumStrValues=null, geometryTypes=null, simplePrimaryKeys=null, primaryKeysWithPrefix=null, enumAndSetDefaultCharset=null, enumAndSetColumnCharsets=null}}}
                TableMapEventData data = event.getData();
                tables.put(data.getTableId(), data.getDatabase() + "." + data.getTable());
            } else if (eventType == EventType.EXT_WRITE_ROWS) {
                // Event{header=EventHeaderV4{timestamp=1685696419000, eventType=EXT_WRITE_ROWS, serverId=1, headerLength=19, dataLength=30, nextPosition=5330, flags=0}, data=WriteRowsEventData{tableId=98, includedColumns={0, 1, 2}, rows=[
                //     [2, 小红, 10]
                // ]}}
                // Event{header=EventHeaderV4{timestamp=1685697239000, eventType=EXT_WRITE_ROWS, serverId=1, headerLength=19, dataLength=26, nextPosition=5618, flags=0}, data=WriteRowsEventData{tableId=98, includedColumns={0, 1, 2}, rows=[
                //     [3, 小, null]
                // ]}}
                WriteRowsEventData data = event.getData();
                String table = tables.get(data.getTableId());
                System.out.println("================");
                System.out.println(table);
                if (table.equals("db1.users")) {
                    List<Serializable[]> rows = data.getRows();
                    for (Serializable[] serializables : rows) {
                        // 字段信息可能会变化，使用ID最靠谱
                        int user_id = (int) serializables[0];
                        try {
                            Statement stmt = conn.createStatement();
                            ResultSet rs = stmt.executeQuery("SELECT user_name FROM " + table + " WHERE user_id = " + user_id);
                            while (rs.next()) {
                                // System.out.println(rs.getString("user_name"));
                                // jedis.set();
                                jedis.setex("user:" + user_id + ":user_name", 3600, rs.getString("user_name"));
                                // 127.0.0.1:6379> keys *
                                // 1) "user:13:user_name"
                                // 2) "user:12:user_name"
                                // 3) "msg"
                                // 127.0.0.1:6379> get user:13:user_name
                                // "test"
                                // 127.0.0.1:6379> ttl user:13:user_name
                                // (integer) 3577
                                // 127.0.0.1:6379>
                            }
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }

                        // HashMap<String, Serializable> row = new HashMap<>();
                        // for (int i = 0; i < serializables.length; i++) {
                        //     String columnName = tableColums.get(table).get(i + 1);
                        //     row.put(columnName, serializables[i]);
                        // }
                        // System.out.println(row);

                    }
                }
                
                // TODO: 修改表的时候，重新获取表字段
            }
        });
        client.connect();

    }
}
