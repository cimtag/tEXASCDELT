package de.cimt.talendcomp.exasol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExecSCD2JUnit {

	static String sourceSchema = "test_sr";
	static String sourceTable = "EMP_NEW";
	static String targetSchema = "test_tg";
	static String targetTable = "EMPLOYEE";
	static EXASCDHelper g;
	static Connection con = null;
	static Statement stmt = null;

	/**
	 * prepare db: delete and create source and target schemas and tables
	 */
	@BeforeClass
	static public void beforeClass() throws Exception {
		String host = null;
		String port = null;
		String schema = null;
		String user = null;
		String password = null;
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
		// con = DriverManager.getConnection("jdbc:exa:192.168.99.100:8563;schema=sys", "sys",
		// "exasol");
		con = DriverManager.getConnection("jdbc:exa:" + host + ":" + port + ";schema=" + schema, user,
				password);
		con.setAutoCommit(true);
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
		g.setCreateTargetTable(true);
		g.setSourceSchema(sourceSchema);
		g.setSourceTable(sourceTable);
		g.setTargetSchema(targetSchema);
		g.setTargetTable(targetTable);
		g.setEnableSCD2Versioning(true);
		g.setValidTimePeriodStartColumn("valid_start");
		g.setValidTimePeriodEndColumn("valid_end");
		g.connect("192.168.99.100", "8563", "sys", "sys", "exasol", null);
		g.setVersionEnabled(true);
		g.setVersionColumn("v_col");
		g.setUseCurrentFlag(true);
		g.setCurrentFlagColumn("cur_flag");
	}

	@Test
	public final void test1() {
		// g.addSourceColumn(name, type, length, precision, nullable, isKey, scd1a, scd1b, scd2, scd3,
		// additionalSCD3Column);
		g.addSourceColumn("EMPNO", "DECIMAL", 30, 0, false, true, false, false, false, false, null);
		g.addSourceColumn("FIRSTNAME", "VARCHAR", 30, 0, false, true, true, false, true, false, null);
		g.addSourceColumn("BIRTHDATE", "DATE", 30, 0, true, false, true, false, false, false, null);
		g.addSourceColumn("SALARY", "DECIMAL", 9, 2, true, false, true, false, true, false, null);

		try {
			boolean success = g.executeAllOperations();
			assertEquals(true, success);
			assertEquals(3, getRowCount(targetSchema + "." + targetTable));
			int[] returnCounterArray = g.getReturnCounters();
			assertEquals(3, returnCounterArray[0]);
			assertEquals(0, returnCounterArray[1]);
			assertEquals(0, returnCounterArray[2]);
			assertEquals(0, returnCounterArray[3]);
			assertEquals(0, returnCounterArray[4]);

			// System.out.println(">>>JUNIT 2");
			// g.executeAllOperations();
			// assertEquals(3, getRowCount(targetSchema + "." + targetTable));
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		assertEquals(1, g.getMaxDuplicatesInSource());
	}

	@Test
	public final void test2() {
		// g.addSourceColumn(name, type, length, precision, nullable, isKey, scd1a, scd1b, scd2, scd3,
		// additionalSCD3Column);
		g.addSourceColumn("EMPNO", "DECIMAL", 30, 0, false, true, false, false, false, false, null);
		g.addSourceColumn("FIRSTNAME", "VARCHAR", 30, 0, false, true, true, false, true, false, null);
		g.addSourceColumn("BIRTHDATE", "DATE", 30, 0, true, false, true, false, false, false, null);
		g.addSourceColumn("SALARY", "DECIMAL", 9, 2, true, false, true, false, true, false, null);

		// change data in staging
		String sql = "update " + sourceSchema + "." + sourceTable + " set "
				+ "SALARY=9999998 where EMPNO=9";
		System.out.println(sql);
		execute(sql);
		try {
			System.out.println(">>>executeAllOperations");
			g.setValidFromDefaultValue((new Timestamp(System.currentTimeMillis() + 10000).toString()));
			g.executeAllOperations();

			assertEquals(4, getRowCount(targetSchema + "." + targetTable));
			int[] returnCounterArray = g.getReturnCounters();
			assertEquals(0, returnCounterArray[0]);
			assertEquals(0, returnCounterArray[1]);
			assertEquals(1, returnCounterArray[2]);
			assertEquals(0, returnCounterArray[3]);
			assertEquals(0, returnCounterArray[4]);
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
		// change data in staging
		sql = "update " + sourceSchema + "." + sourceTable + " set " + "SALARY=9999997 where EMPNO=9";
		// System.out.println(sql);
		execute(sql);
		try {
			System.out.println(">>>executeAllOperations");
			g.executeAllOperations();
			assertEquals(4, getRowCount(targetSchema + "." + targetTable));
			int[] returnCounterArray = g.getReturnCounters();
			assertEquals(0, returnCounterArray[0]); // old counter is kept
			assertEquals(0, returnCounterArray[1]);
			assertEquals(1, returnCounterArray[2]);
			assertEquals(0, returnCounterArray[3]);
			assertEquals(1, returnCounterArray[4]);
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
			String sql = "drop schema " + sourceSchema + " CASCADE";
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
