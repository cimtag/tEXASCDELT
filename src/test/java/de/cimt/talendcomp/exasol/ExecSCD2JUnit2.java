package de.cimt.talendcomp.exasol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.SQLException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExecSCD2JUnit2 extends AbstractTestBase {


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
		g.setDebug(false);
		g.setCreateTargetTable(true);
		g.setSourceSchema(sourceSchema);
		g.setSourceTable(sourceTable);
		g.setTargetSchema(targetSchema);
		g.setTargetTable(targetTable);
		g.setEnableSCD2Versioning(true);
		g.setValidTimePeriodStartColumn("valid_start");
		g.setValidTimePeriodEndColumn("valid_end");
		g.connect(host, port, schema, user, password, null);
		g.setVersionEnabled(true);
		g.setVersionColumn("version");
		g.setUseCurrentFlag(true);
		g.setCurrentFlagColumn("cur");
		g.addAdditionalColumn("foo", "VARCHAR", 30, 0, true, true, true);
	}

	@Test
	public final void test1() {
		// g.addSourceColumn(name, type, length, precision, nullable, isKey, scd1a, scd1b, scd2, scd3,
		// additionalSCD3Column);
		g.addSourceColumn("EMPNO", "DECIMAL", 30, 0, false, true, false, false, false, false, null);
		g.addSourceColumn("FIRSTNAME", "VARCHAR", 30, 0, true, false, false, false, true, false, null);

		try {
			System.out.println(">>>executeAllOperations");

			boolean success = g.executeAllOperations();
			assertEquals(true, success);
			assertEquals(3, getRowCount(targetSchema + "." + targetTable));
			int[] returnCounterArray = g.getReturnCounters();
			assertEquals(3, returnCounterArray[0]);
			assertEquals(0, returnCounterArray[1]);
			assertEquals(0, returnCounterArray[2]);
			assertEquals(0, returnCounterArray[3]);
			assertEquals(0, returnCounterArray[4]);

			success = g.executeAllOperations();
			assertEquals(true, success);
			assertEquals(3, getRowCount(targetSchema + "." + targetTable));
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		try {
			assertEquals(1, g.getMaxDuplicatesInSource(false));
		} catch (SQLException e) {
			fail();
		}

	}

	static void createTable(String sourceSchema, String sourceTable) {
		String sql = "create table " + sourceSchema + "." + sourceTable
				+ " (EMPNO DECIMAL(30,0) NOT NULL, FIRSTNAME VARCHAR(15))";
		execute(sql);
		sql = "insert into " + sourceSchema + "." + sourceTable + " values " + "(2,'Luke'),"
				+ "(5,'Obi-Wan')," + "(9,'Yoda')";
		System.out.println(sql);
		execute(sql);
	}

}
