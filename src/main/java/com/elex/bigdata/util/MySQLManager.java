package com.elex.bigdata.util;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public final class MySQLManager {
    private static Map<String,String> mysqlConf ;

    static {
        try {
            PropertiesConfiguration prop = new PropertiesConfiguration("mysql_config.properties");
            mysqlConf = new HashMap<String, String>();

            mysqlConf.put("thor.url", prop.getString("thor.url"));
            mysqlConf.put("thor.username", prop.getString("thor.username"));
            mysqlConf.put("thor.password", prop.getString("thor.password"));

            mysqlConf.put("odin.url", prop.getString("odin.url"));
            mysqlConf.put("odin.username", prop.getString("odin.username"));
            mysqlConf.put("odin.password", prop.getString("odin.password"));
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new ExceptionInInitializerError(e);
        } catch (ConfigurationException e) {
            throw new RuntimeException("Error when parse the mysql configuration", e);
        }
    }

	public static Connection getConnection(String type) throws Exception {
		return DriverManager.getConnection(mysqlConf.get(type + ".url"),
                mysqlConf.get(type + ".username"), mysqlConf.get(type + ".password"));
	}


	public static void close(ResultSet rs, Statement st, Connection conn) {
		try {
			if (rs != null)
				rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (st != null)
					st.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				if (conn != null)
					try {
						conn.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
			}
		}
	}

}