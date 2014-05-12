package com.elex.bigdata.uid;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: Z J Wu Date: 14-1-7 Time: 下午4:08 Package: com.xingcloud.uidtransform
 */
public enum StreamLogUidTransformer {
    INSTANCE;

    private BasicDataSource ds;

    private final int BATCH_SIZE = 50;

    private final char NEW_LINE = '\n';
    private final String EMPTY_LINE = "";

    private void close(AutoCloseable autoCloseable) {
        if (autoCloseable != null) {
            try {
                autoCloseable.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private StreamLogUidTransformer() {
        ds = new BasicDataSource();
        Collection<String> initSql = new ArrayList<String>(1);
        initSql.add("select 1;");
        ds.setConnectionInitSqls(initSql);

        ds.setDriverClassName("com.mysql.jdbc.Driver");
        ds.setUsername("xingyun");
        ds.setPassword("xa");
        ds.setUrl("jdbc:mysql://65.255.35.134");
    }


    public Map<Long, String> executeSqlTrue(String projectId, long[] uids) throws SQLException {
        if (ArrayUtils.isEmpty(uids)) {
            return null;
        }
        String[] strings = new String[uids.length];
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        StringBuilder sql = new StringBuilder("select t.id, t.orig_id from `vf_");
        sql.append(projectId);
        sql.append("`.id_map as t where t.id in (?");
        char comma = ',';
        for (int i = 1; i < uids.length; i++) {
            sql.append(comma);
            sql.append("?");
        }
        sql.append(')');
        Map<Long, String> idmap = new HashMap<Long, String>(uids.length);
        try {
            conn = ds.getConnection();
            pstmt = conn.prepareStatement(sql.toString());
            for(int i=1;i<=uids.length;i++){
                pstmt.setLong(i,uids[i-1]);
            }
      System.out.println(sql.toString());
            rs = pstmt.executeQuery();
            while (rs.next()) {
                idmap.put(rs.getLong(1), rs.getString(2));
            }
        } finally {
            close(conn);
            close(pstmt);
            close(rs);
        }

        return idmap;
    }

    public Map<Long, String> executeSqlTrue(String projectId, String[] uids) throws SQLException {
        if (ArrayUtils.isEmpty(uids)) {
            return null;
        }
        String[] strings = new String[uids.length];
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        StringBuilder sql = new StringBuilder("select t.id, t.orig_id from `vf_");
        sql.append(projectId);
        sql.append("`.id_map as t where t.orig_id in (?");
        char comma = ',';
        for (int i = 1; i < uids.length; i++) {
            sql.append(comma);
            sql.append("?");
        }
        sql.append(')');
        Map<Long, String> idmap = new HashMap<Long, String>(uids.length);
        try {
            conn = ds.getConnection();
            pstmt = conn.prepareStatement(sql.toString());
            for(int i=1;i<=uids.length;i++){
                pstmt.setString(i,uids[i-1]);
            }

            System.out.println(sql.toString());
            rs = pstmt.executeQuery();
            while (rs.next()) {
                idmap.put(rs.getLong(1), rs.getString(2));
            }
        } finally {
            close(conn);
            close(pstmt);
            close(rs);
        }

        return idmap;
    }

    public Map<Long, String> transform(String projectId, List<Long> internalUIDs) throws IOException,
            SQLException {
        int size = internalUIDs.size();
        long[] internalUIDArray = new long[size];
        for (int i = 0; i < size; i++) {
            internalUIDArray[i] = internalUIDs.get(i);
        }
        Map<Long, String> idMap = executeSqlTrue(projectId, internalUIDArray);
        return idMap;
    }

    public Map<Long, String> transform(String projectId, String[] orgUIDs) throws IOException,
            SQLException {

        Map<Long, String> idMap = executeSqlTrue(projectId, orgUIDs);
        return idMap;
    }

}
