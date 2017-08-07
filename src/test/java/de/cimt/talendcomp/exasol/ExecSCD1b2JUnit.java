package de.cimt.talendcomp.exasol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.ResultSet;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExecSCD1b2JUnit extends AbstractTestBase {

	/**
	 * prepare db: delete and create source and target schemas and tables
	 */
	@BeforeClass
	static public void beforeClass() throws Exception {
		setupBase();
		createTable(sourceSchema, sourceTable);
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
		g.setTimestampInSourceColumn("val_start");
		g.connect(host, port, schema, user, password, null);
		g.setVersionEnabled(false);
//		g.setVersionColumn("v_col");
		g.setUseCurrentFlag(false);
//		g.setCurrentFlagColumn("cur_flag");
	}

	@Test
	public final void test1() {
		// g.addSourceColumn(name, type, length, precision, nullable, isKey, scd1a, scd1b, scd2, scd3,
		// additionalSCD3Column);
		g.addSourceColumn("the_key", "DECIMAL", 30, 0, false, true, false, false, false, false, null);
		g.addSourceColumn("scd0", "VARCHAR", 30, 0, false, false, false, false, false, false, null);
		g.addSourceColumn("scd1b", "VARCHAR", 30, 0, false, false, false, true, false, false, null);
		g.addSourceColumn("scd2", "VARCHAR", 30, 0, false, false, false, false, true, false, null);

		try {
			boolean success = g.executeAllOperations();
			assertEquals(true, success);
			String sql = "select * from " + targetSchema + "." + targetTable;
			ResultSet rs = stmt.executeQuery(sql);
			rs.next();
			assertEquals("a", rs.getString("SCD0"));
			assertEquals("bb", rs.getString("SCD1B"));
			assertEquals("c", rs.getString("SCD2"));
			rs.next();
			assertEquals("aa", rs.getString("SCD0"));
			assertEquals("bb", rs.getString("SCD1B"));
			assertEquals("cc", rs.getString("SCD2"));
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

	}

	static void createTable(String sourceSchema, String sourceTable) {
		String sql = "create table " + sourceSchema + "." + sourceTable
				+ " (the_key DECIMAL(30,0) NOT NULL, val_start DATE, scd0 VARCHAR(30), scd1b VARCHAR(30), scd2 VARCHAR(30))";

		execute(sql);
		sql = "insert into " + sourceSchema + "." + sourceTable + " values "
				+ "(42,'2017-01-02','a','x','c'),(42,'2017-01-03','x','xx','c'),"
				+ "(42,'2017-01-04','aa','xxx','cc'),(42,'2017-01-05','y','bb','cc')";
		// System.out.println(sql);
		execute(sql);
	}

}
