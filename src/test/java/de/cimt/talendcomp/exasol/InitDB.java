package de.cimt.talendcomp.exasol;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class InitDB {

	public static void setupTable(String sourceSchema, String sourceTable) {
		dropSchema(sourceSchema);
		try {
			Class.forName("com.exasol.jdbc.EXADriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		Connection con = null;
		Statement stmt = null;
		try {
			con = DriverManager.getConnection("jdbc:exa:192.168.99.100:8563;schema=sys", "sys", "exasol");
			stmt = con.createStatement();
			String sql = "create schema " + sourceSchema;
			System.out.println("#InitDB: " + sql);
			stmt.execute(sql);
			sql = "create table " + sourceSchema + "." + sourceTable
					+ " (EMPNO DECIMAL(30,0) NOT NULL, FIRSTNAME VARCHAR(15) NOT NULL, BIRTHDATE DATE, SALARY DECIMAL(9,2))";
			System.out.println(sql);
			stmt.execute(sql);
			System.out.println("#InitDB: " + "source table created");
			sql = "insert into " + sourceSchema + "." + sourceTable + " values "
					+ "(2,'Luke','1960-02-13',123456.78)," + "(5,'Obi-Wan','1922-12-15',5624534),"
					+ "(9,'Yoda','1083-02-15',9999999)";
			System.out.println("#InitDB: " + sql);
			stmt.execute(sql);
			System.out.println("#InitDB: " + "data inserted");

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				stmt.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			try {
				con.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	public static void dropSchema(String sourceSchema) {
		try {
			Class.forName("com.exasol.jdbc.EXADriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		Connection con = null;
		Statement stmt = null;
		try {
			con = DriverManager.getConnection("jdbc:exa:192.168.99.100:8563;schema=sys", "sys", "exasol");
			stmt = con.createStatement();
			String sql = "drop schema " + sourceSchema + " CASCADE";
			System.out.println("#InitDB: " + sql);
			stmt.execute(sql);
			System.out.println("#InitDB: " + "schema dropped");
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				stmt.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			try {
				con.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

}
