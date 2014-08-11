package com.elex.ba.main;

import com.elex.ba.util.Utils;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Author: liqiang
 * Date: 14-8-5
 * Time: 下午6:51
 */
public class ScanHBaseUID {

    private BasicDataSource ds;

    public static void main(String[] args) throws Exception{

        String pid = args[0];
        String startKey = args[1];
        String endKey = args[2];

        ExecutorService service = Executors.newFixedThreadPool(16);
        List<Future<List<Long>>> tasks = new ArrayList<Future<List<Long>>>();

        for(int i=0;i<16;i++){
            tasks.add(service.submit(new ScanUID("node" + i,pid,startKey,endKey)));
        }

        Set<Long> allUid = new HashSet<Long>();

        int all = 0;

        for(Future<List<Long>> uids : tasks){
            try{
                List<Long> rs = uids.get();
                all += rs.size();
                allUid.addAll(rs);
            }catch(Exception e){
                e.printStackTrace();
            }
        }

        ScanHBaseUID shu = new ScanHBaseUID();

        Map<Long,String> result = shu.executeSqlTrue(pid,allUid);

        for(Map.Entry<Long,String> s : result.entrySet()){
            System.out.println(s.getKey() + " " + s.getValue());
        }
        System.out.println("All count : "  + all);
        System.out.println("Uniq count : " + allUid.size());
        service.shutdownNow();

    }

    public Map<Long, String> executeSqlTrue(String projectId, Set<Long> uids) throws SQLException {
        if (uids.size() == 0) {
            return null;
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        StringBuilder sql = new StringBuilder("select t.id, t.orig_id from `vf_");
        sql.append(projectId);
        sql.append("`.id_map as t where t.id in (?");
        char comma = ',';
        for (int i = 1; i < uids.size(); i++) {
            sql.append(comma);
            sql.append("?");
        }
        sql.append(')');
        Map<Long, String> idmap = new HashMap<Long, String>(uids.size());
        try {
            conn = ds.getConnection();
            pstmt = conn.prepareStatement(sql.toString());
            int i = 1;
            for(long uid : uids){
                pstmt.setLong(i,uid);
                i++;
            }
            rs = pstmt.executeQuery();
            while (rs.next()) {
                idmap.put(rs.getLong(1), rs.getString(2));
            }
        } finally {
            conn.close();
            pstmt.close();
            rs.close();
        }

        return idmap;
    }

    private ScanHBaseUID() {
        ds = new BasicDataSource();
        Collection<String> initSql = new ArrayList<String>(1);
        initSql.add("select 1;");
        ds.setConnectionInitSqls(initSql);

        ds.setDriverClassName("com.mysql.jdbc.Driver");
        ds.setUsername("xingyun");
        ds.setPassword("xa");
        ds.setUrl("jdbc:mysql://65.255.35.134");
    }

}

class ScanUID implements Callable<List<Long>>{

    String node;
    String tableName;
    String startKey;
    String endKey;

    public ScanUID(String node,String tableName,String startKey,String endKey){
        this.node = node;
        this.tableName = tableName;
        this.startKey = startKey;
        this.endKey = endKey;
    }

    @Override
    public List<Long> call() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", node);
        conf.set("hbase.zookeeper.property.clientPort", "3181");

        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startKey));
        scan.setStopRow(Bytes.toBytes(endKey));
        scan.setMaxVersions(1); //只需要一个version
        scan.setCaching(10000);
        scan.setFilter(new KeyOnlyFilter());

        HTable table = new HTable(conf,"deu_" + tableName);
        ResultScanner scanner = table.getScanner(scan);
        List<Long> results = new ArrayList<Long>();
        try{
            for(Result r : scanner){
                long uid = Utils.transformerUID(Bytes.tail(r.getRow(), 5));
                results.add(Utils.truncate(uid));
            }
        }finally {
            scanner.close();
            table.close();
        }
        return results;
    }
}

