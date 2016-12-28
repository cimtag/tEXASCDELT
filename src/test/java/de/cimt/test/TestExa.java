package de.cimt.test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

public class TestExa {

	static String sourceSchema = "test_sr";
	static String sourceTable = "EMP_NEW";

	/**
	 * @param args
	 * @throws SQLException
	 */
	public static void main(String[] args) throws SQLException {
		initSource();

	}

	public static void initSource() throws SQLException {
		try {
			Class.forName("com.exasol.jdbc.EXADriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		Connection con = null;
		Statement stmt = null;
		con = DriverManager.getConnection("jdbc:exa:192.168.99.100:8563;schema=sys", "sys", "exasol");
		stmt = con.createStatement();

		con.setAutoCommit(false);
		try {
			stmt.executeQuery("drop schema tn cascade");
		} catch (SQLException e) {
		}

		try {
			String sql = "create schema tn";
			stmt.addBatch(sql);
			sql = "create table tn.tab (NAME VARCHAR(15) NOT NULL, BIRTHDATE DATE, TS TIMESTAMP)";
			stmt.addBatch(sql);
			Date d = new Date(System.currentTimeMillis());
			Timestamp t = new Timestamp(d.getTime());
			System.out.println(t);
			// sql = "insert into tn.tab (NAME, BIRTHDATE, TS) values ('foo', '1981-06-30','" + d + "');";
			sql = "insert into tn.tab (NAME, BIRTHDATE, TS) values ('foo', '1981-06-30','2016-12-17 11:44:01.123456');";
			stmt.addBatch(sql);

			int[] a = stmt.executeBatch();
			System.out.println("length=" + a.length);
			for (int i : a) {
				System.out.println(i);
			}
			ResultSet rs = stmt.executeQuery("select * from tn.tab");
			System.out.println(">>>ResultSet");
			while(rs.next()) {
				System.out.println(rs.getString("NAME"));
				System.out.println(rs.getDate("BIRTHDATE"));
				System.out.println(rs.getTimestamp("TS"));
			}

			// con.rollback();
			con.commit();
			
			

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
