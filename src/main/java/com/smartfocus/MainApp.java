package com.smartfocus;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.PropertySource;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

@ComponentScan("com.smartfocus")
@PropertySource(value = "file:global.properties")
@SpringBootApplication
public class MainApp extends ParentHBaseDAO {

    @Value("${hbase.zookeeper.quorumPeer:localhost}")
    private String zookeeperQuorumPeer;
    @Value("${hbase.zookeeper.port:2181}")
    private int zookeeperPort;
    @Value("${hbase.connection.timeout:12000}")
    private int timeout;
    //@Value("${batch.size}")
    //private int batchSize;
    @Value("${hbase.rpc.timeout:1800000}")
    private int rpcTimeout;
    @Value("${hbase.client.scanner.timeout.period:60000}")
    private int scannerTimeout;
    @Value("${hbase.scan.batch:20}")
    private int scanBatch;
    @Value("${number.of.records.per.run}")
    private long numberOfRecordsPerRun;

    private static final byte[] DATA_CF = toBytes("d");
    @Value("${hbase.database:IPS3StandAlone}")
    private String database;
    private Configuration conf;
    @Autowired
    private ConsoleProgressBar cpb;

    @Value("${commit.on.delete:false}")
    private boolean commitOnDelete;
    @Value("${number.of.iteration:1}")
    private int numberOfIteration;

    private static long totalRecordsProcessed = 0;

    private Configuration getHBaseConf() {
        Configuration conf = HBaseConfiguration.create();
        conf.setInt("timeout", timeout);
        //conf.set("hbase.master", hbaseHostUrl);
        conf.set("hbase.zookeeper.quorum", zookeeperQuorumPeer);
        conf.setInt("hbase.zookeeper.property.clientPort", zookeeperPort);
        conf.setInt("hbase.rpc.timeout", rpcTimeout);
        conf.setInt("hbase.client.scanner.timeout.period", scannerTimeout);
        return conf;
    }

    public static void main(String[] args) throws Exception {

        ApplicationContext ctx = SpringApplication.run(MainApp.class, args);
        MainApp app = ctx.getBean(MainApp.class);

        Configuration conf = app.getHBaseConf();
        System.out.println("hbase configuration: " + conf);

        app.conf = conf;
        for(int i = 0; i < app.numberOfIteration; i++) {
            if(app.run()) {
                break;
            }
        }
        System.out.println("Total records processed " + totalRecordsProcessed);
    }

    private byte[] startRow = null;
    private String startRowFilename = "start-row-marker.bin";

    private byte[] getStartRow() {
        if (startRow == null) {
            //load from file
            FileInputStream fis = null;
            try {
                fis = new FileInputStream(new File(startRowFilename));
                byte[] buf = new byte[256];
                int count = fis.read(buf);
                return Bytes.copy(buf, 0, count);
            } catch (FileNotFoundException notfound) {
                //fine
            } catch (Exception io) {
                throw new RuntimeException(io);
            } finally {
                if (fis != null) {
                    try {
                        fis.close();
                    } catch (IOException e) {
                        //noop
                    }
                }
            }
        }
        return null;
    }

    private boolean saveStartRow(byte[] startRow) {
        if (startRow == null || startRow.length == 0) {
            throw new IllegalArgumentException("startRow must not be null");
        }
        //load from file
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(new File(startRowFilename));
            fos.write(startRow);
            return true;
        } catch (Exception io) {
            throw new RuntimeException(io);
        } finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    //noop
                }
            }
        }
    }


    private boolean run() throws Exception {
        long start = System.currentTimeMillis();

        // Instantiating the Scan class
        Scan scan = new Scan();
        scan.addFamily(DATA_CF);
        scan.addColumn(DATA_CF, toBytes("f_iid"));
        scan.addColumn(DATA_CF, toBytes("f_paid"));

        byte[] startRowMarker = getStartRow();
        if (startRowMarker != null) {
            cpb.println("Found marker : " + Bytes.toStringBinary(startRowMarker));
            //restart where marked
            scan.setStartRow(startRowMarker);
        } else {
            cpb.println("No marker found.");
        }

        // Instantiating HTable class
        HTable table = new HTable(conf, database + "_Representations");
        ResultScanner scanner = table.getScanner(scan);

        Scan userScan = new Scan();
        userScan.addFamily(DATA_CF);
        HTable userTable = new HTable(conf, database + "_Users");
        byte[] rowMarker = null;
        long count = 0;
        try {
            List<Delete> toBeDeleted = new ArrayList<>();
            Result result;
            long recordsAffected = 0;

            cpb.println("Start scanning results...");
            while ((result = scanner.next()) != null) {
                totalRecordsProcessed++;
                count++;
                byte[] currentRow = result.getRow();
                if (count > numberOfRecordsPerRun) {
                    break;
                }

                String scannerRowKey = Bytes.toStringBinary(currentRow);
                cpb.setStatus(totalRecordsProcessed, "Processing..." + scannerRowKey);
                byte[] iid = result.getValue(DATA_CF, toBytes("f_iid"));
                byte[] paid = result.getValue(DATA_CF, toBytes("f_paid"));
                //cpb.println(Hex.encodeHexString(paid) + "+" + Hex.encodeHexString(iid));

                byte[] userRowKey = joinBytes(Bytes.copy(paid, 4, paid.length - 4), iid);

                //get/search the users table
                Result userResult = userTable.get(new Get(userRowKey));
                if (userResult.getRow() == null) {
                    System.out.println("==>Registrations rowkey: " +scannerRowKey);
                    System.out.println("\tf_paid:" + Bytes.toStringBinary(paid) + " ,f_iid:" + Bytes.toStringBinary(iid));
                    System.out.println("\tSearching Users rowkey   '" + Bytes.toStringBinary(userRowKey) + "' Not found\n");
                    toBeDeleted.add(new Delete(currentRow));
                    recordsAffected++;
                }
                rowMarker = currentRow;
            }
            if(commitOnDelete) {
                System.out.println(toBeDeleted.size() + " records deleted!");
                table.delete(toBeDeleted);
            }
            cpb.println("===>>> ElapsedTime in ms: [" + (System.currentTimeMillis() - start) + "] " + recordsAffected);
        } finally {
            if (rowMarker != null) {
                saveStartRow(rowMarker);
                cpb.println("Marker saved #" + Bytes.toStringBinary(rowMarker));
            }
        }

        table.flushCommits();
        scanner.close();
        table.close();
        return Bytes.equals(startRowMarker, rowMarker);
    }
}
