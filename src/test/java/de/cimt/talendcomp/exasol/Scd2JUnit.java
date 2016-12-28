package de.cimt.talendcomp.exasol;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class Scd2JUnit {

	static String sourceSchema = "test_sr_scd1";
	static String sourceTable = "EMP_NEW";
	static String targetSchema = "test_tg_scd1";
	static String targetTable = "EMPLOYEE";
	static EXASCDHelper g;
	static Connection con = null;
	static Statement stmt = null;

	/**
	 * setup db
	 */
	@BeforeClass
	static public void beforeClass() throws Exception {
		System.out.println(1);
		try {
			Class.forName("com.exasol.jdbc.EXADriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		con = DriverManager.getConnection("jdbc:exa:192.168.99.100:8563;schema=sys", "sys", "exasol");
		stmt = con.createStatement();
		dropSchema(sourceSchema);
		createSchema(sourceSchema);
		createTable(sourceSchema, sourceTable);
		dropSchema(targetSchema);
		createSchema(targetSchema);
	}

	@AfterClass
	static public void afterClass() throws Exception {
		stmt.close();
		con.close();
	}

	/**
	 * setup merger object
	 */
	@Before
	public void setUp() throws Exception {
		g = new EXASCDHelper();
		g.setDebug(true);
		g.setSourceSchema(sourceSchema);
		g.setSourceTable(sourceTable);
		g.setTargetSchema(targetSchema);
		g.setTargetTable(targetTable);
		g.setEnableSCD2Versioning(true);
		g.setValidTimePeriodStartColumn("valid_start");
		g.setValidTimePeriodEndColumn("valid_end");
		g.connect("192.168.99.100", "8563", "sys", "sys", "exasol", null);
	}

	@Test
	public final void testSCD2() {
		// g.addSourceColumn(name, type, length, precision, nullable, isKey, scd1a, scd1b, scd2, scd3,
		// additionalSCD3Column);
		g.addSourceColumn("EMPNO", "DECIMAL", 30, 0, false, true, false, false, false, false, null);
		g.addSourceColumn("FIRSTNAME", "VARCHAR", 30, 0, true, false, false, false, true, false, null);
		g.addSourceColumn("BIRTHDATE", "DATE", 30, 0, true, false, false, false, false, false, null);
		g.addSourceColumn("SALARY", "DECIMAL", 9, 2, true, false, false, false, false, false, null);

		try {
			g.executeCreateTargetTable();
			boolean b = g.executeAllOperations();
			assertEquals(true, b);
			// numberOfLines = g.executeMerge();
			// assertEquals(0, numberOfLines);
			// System.out.println("Modified lines:" + numberOfLines);
			//
			// String sql = "update " + sourceSchema + "." + sourceTable
			// + " set firstname='foo' where empno=2";
			// execute(sql);
			// numberOfLines = g.executeMerge();
			// assertEquals(1, numberOfLines);
			// System.out.println("Modified lines:" + numberOfLines);

		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}

	static void createTable(String sourceSchema, String sourceTable) {
		String sql = "create table " + sourceSchema + "." + sourceTable
				+ " (EMPNO DECIMAL(30,0) NOT NULL, FIRSTNAME VARCHAR(15) NOT NULL, BIRTHDATE DATE, SALARY DECIMAL(9,2))";
		execute(sql);
		sql = "insert into " + sourceSchema + "." + sourceTable + " values "
				+ "(2,'Luke','1960-02-13',123456.78)," + "(5,'Obi-Wan','1922-12-15',5624534),"
				+ "(9,'Yoda','1083-02-15',9999999)";
		System.out.println(sql);
		execute(sql);
	}

	static void execute(String sql) {
		try {
			System.out.println(sql);
			stmt.execute(sql);
			System.out.println("#executed");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	static void createSchema(String sourceSchema) {
		try {
			String sql = "create schema " + sourceSchema;
			System.out.println(sql);
			stmt.execute(sql);
			System.out.println("#schema created");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	static void dropSchema(String sourceSchema) {
		try {
			String sql = "drop schema " + sourceSchema + " CASCADE";
			System.out.println(sql);
			stmt.execute(sql);
			System.out.println("#schema dropped");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
