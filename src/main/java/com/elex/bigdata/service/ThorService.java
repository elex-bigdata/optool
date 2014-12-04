package com.elex.bigdata.service;

import com.elex.bigdata.util.MySQLManager;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

/**
 * Author: liqiang
 * Date: 14-12-4
 * Time: 下午1:19
 */
public class ThorService {

    public String getCodeByID(int id) throws Exception {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try{
            String sql = "select code  from ad_info where id=" + id;
            conn = MySQLManager.getConnection("odin");
            stmt = conn.createStatement();
            System.out.println(sql);
            rs = stmt.executeQuery(sql);
            if(rs.next()){
                return rs.getString(1);
            }
        }catch (Exception e){
            throw new Exception("Error when get the code from mysql",e);
        } finally {
            MySQLManager.close(rs, stmt, conn);
        }
        return null;
    }


    public Map<String,String> getADs() throws Exception {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        Map<String,String> result  = new LinkedHashMap<String,String>();
        try{
            String sql = "select id, name  from ad_info order by name ";
            conn = MySQLManager.getConnection("odin");
            stmt = conn.createStatement();
            System.out.println(sql);
            rs = stmt.executeQuery(sql);
            while(rs.next()){
                result.put(rs.getString(1),rs.getString(2));
            }
        }catch (Exception e){
            throw new Exception("Error when get the code from mysql",e);
        } finally {
            MySQLManager.close(rs, stmt, conn);
        }
        return null;
    }
}
