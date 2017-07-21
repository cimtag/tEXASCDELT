package de.cimt.talendcomp.exasol;

import static org.junit.Assert.fail;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public abstract class AbstractTestBase {
	static String sourceSchema = "test_sr";
	static String sourceTable = "EMP_NEW";
	static String targetSchema = "test_tg";
	static String targetTable = "EMPLOYEE";
	static EXASCDHelper g;
	static Connection con = null;
	static Statement stmt = null;
	static String host = null;
	static String port = null;
	static String schema = null;
	static String user = null;
	static String password = null;

	static void setupBase() throws SQLException {
		Properties prop = new Properties();
		InputStream input = null;
		try {
			input = new FileInputStream("./resources/exa_con.properties");
			prop.load(input);
			host = prop.getProperty("host");
			port = prop.getProperty("port");
			schema = prop.getProperty("schema");
			user = prop.getProperty("user");
			password = prop.getProperty("password");
		} catch (IOException ex) {
			ex.printStackTrace();
			fail();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		try {
			Class.forName("com.exasol.jdbc.EXADriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			fail();
		}
		// con =
		// DriverManager.getConnection("jdbc:exa:192.168.99.100:8563;schema=sys",
		// "sys", "exasol");
		try {
			con = DriverManager.getConnection("jdbc:exa:" + host + ":" + port + ";schema=" + schema, user, password);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		con.setAutoCommit(true);
		stmt = con.createStatement();
		dropSchema(sourceSchema);
		createSchema(sourceSchema);
		dropSchema(targetSchema);
		createSchema(targetSchema);
	}

	static void execute(String sql) {
		try {
			stmt.execute(sql);
			System.out.println("#executed: " + sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	static void createSchema(String sourceSchema) {
		try {
			String sql = "create schema " + sourceSchema;
			stmt.execute(sql);
			System.out.println("#schema created: " + sourceSchema);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	static void dropSchema(String sourceSchema) {
		try {
			String sql = "drop schema if exists " + sourceSchema + " CASCADE";
			stmt.execute(sql);
			System.out.println("#schema dropped: " + sourceSchema);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	static int getRowCount(String schemaTable) {
		try {
			String sql = "select count(*) from " + schemaTable;
			ResultSet rs = stmt.executeQuery(sql);
			rs.next();
			return rs.getInt(1);
		} catch (SQLException e) {
			e.printStackTrace();
			return -42;
		}
	}
}
