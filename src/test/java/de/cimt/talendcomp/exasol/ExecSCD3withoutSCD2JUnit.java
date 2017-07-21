package de.cimt.talendcomp.exasol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExecSCD3withoutSCD2JUnit extends AbstractTestBase {

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
		g.setEnableSCD2Versioning(false);
		g.setTimeOfLastSCD3ChangeColumn("scd3change_at");
		g.connect(host, port, schema, user, password, null);
	}

	@Test
	public final void test1() {
		// g.addSourceColumn(name, type, length, precision, nullable, isKey,
		// scd1a, scd1b, scd2, scd3,
		// additionalSCD3Column);
		g.addSourceColumn("EMPNO", "DECIMAL", 30, 0, false, true, false, false, false, false, null);

		// g.addSourceColumn("FIRSTNAME", "VARCHAR", 30, 0, false, true, false,
		// false, false, false,
		// null);
		g.addSourceColumn("FIRSTNAME", "VARCHAR", 30, 0, true, false, false, false, false, false, null);

		g.addSourceColumn("BIRTHDATE", "DATE", 30, 0, true, false, false, false, false, false, null);
		g.addSourceColumn("SALARY", "DECIMAL", 9, 2, true, false, false, false, false, true, "current_salary");
		// g.addSourceColumn("SALARY", "DECIMAL", 9, 2, true, false, false,
		// false, false, false,
		// null);

		try {
			System.out.println(">>>executeAllOperations");
			g.executeAllOperations();
			assertEquals(3, getRowCount(targetSchema + "." + targetTable));
			int[] a = { 0, 3, 0, 0, 0 };
			assertEquals(true, Arrays.equals(a, g.getReturnCounters()));
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		// change data in staging
		String sql = "update " + sourceSchema + "." + sourceTable + " set " + "SALARY=9999998 where EMPNO=9";
		// System.out.println(sql);
		execute(sql);
		try {
			g.setValidFromDefaultValue("2016-12-23");
			System.out.println(">>>executeAllOperations");
			g.executeAllOperations();
			assertEquals(3, getRowCount(targetSchema + "." + targetTable));
			int[] a = { 0, 3, 0, 1, 0 };// counters add up; that's why 3 for
										// inserts stays
			assertEquals(true, Arrays.equals(a, g.getReturnCounters()));
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
		// change data in staging
		sql = "update " + sourceSchema + "." + sourceTable + " set " + "SALARY=9999997 where EMPNO=9";
		// System.out.println(sql);
		execute(sql);
		try {
			g.setValidFromDefaultValue("2016-12-23 12");
			System.out.println(">>>executeAllOperations");
			g.executeAllOperations();
			assertEquals(3, getRowCount(targetSchema + "." + targetTable));
			int[] a = { 0, 3, 0, 2, 0 };// counters add up; that's why 3 for
										// inserts stays
			assertEquals(true, Arrays.equals(a, g.getReturnCounters()));
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}

	static void createTable(String sourceSchema, String sourceTable) {
		String sql = "create table " + sourceSchema + "." + sourceTable
				+ " (EMPNO DECIMAL(30,0) NOT NULL, FIRSTNAME VARCHAR(15) NOT NULL, BIRTHDATE DATE, SALARY DECIMAL(9,2))";
		execute(sql);
		sql = "insert into " + sourceSchema + "." + sourceTable + " values " + "(2,'Luke','1960-02-13',123456.78),"
				+ "(5,'Obi-Wan','1922-12-15',5624534)," + "(9,'Yoda','1083-02-15',9999999)";
		System.out.println(sql);
		execute(sql);
	}

}
