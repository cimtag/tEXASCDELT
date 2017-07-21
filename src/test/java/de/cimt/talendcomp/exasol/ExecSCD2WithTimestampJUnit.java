package de.cimt.talendcomp.exasol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExecSCD2WithTimestampJUnit extends AbstractTestBase {

	static boolean withSCD3 = true;

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
		g.setEnableLogStatements(true);
		g.setStatementsLogFile("C:/var/log.txt", null);

		g.setCreateTargetTable(true);
		g.setBuildTargetTableWithPk(true);
		g.setSourceSchema(sourceSchema);
		g.setSourceTable(sourceTable);
		g.setTargetSchema(targetSchema);
		g.setTargetTable(targetTable);
		g.setEnableSCD2Versioning(true);
		g.setValidTimePeriodStartColumn("valid_start");
		g.setValidTimePeriodEndColumn("valid_end");
		g.setScdEndDate("9999-02-02");
		g.setValidFromDefaultValue("'1234-12-23'");
		g.connect(host, port, schema, user, password, null);
		g.setVersionEnabled(true);
		g.setVersionColumn("vers_col");
		g.setTimestampInSourceColumn("TS1");
	}

	@Test
	public final void test1() {
		// g.addSourceColumn(name, type, length, precision, nullable, isKey, scd1a, scd1b, scd2, scd3,
		// additionalSCD3Column);
		g.addSourceColumn("EMPNO", "DECIMAL", 30, 0, false, true, false, false, false, false, null);
		g.addSourceColumn("FIRSTNAME", "VARCHAR", 30, 0, false, true, false, false, false, false, null);
		g.addSourceColumn("BIRTHDATE", "DATE", 30, 0, true, false, false, true, true, false, null);
		if (withSCD3) {
			g.addSourceColumn("SALARY", "DECIMAL", 9, 2, true, false, false, false, false, true,
					"current_salary");
		} else {
			g.addSourceColumn("SALARY", "DECIMAL", 9, 2, true, false, false, true, false, false, null);
		}
		try {
			System.out.println(">>>executeAllOperations");
			g.executeAllOperations();
			assertEquals(4, getRowCount(targetSchema + "." + targetTable));
			g.executeAllOperations();
			assertEquals(4, getRowCount(targetSchema + "." + targetTable));
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		// change data in staging
		String sql = "update " + sourceSchema + "." + sourceTable + " set "
				+ "SALARY=9999998, TS1='2016-12-14' where EMPNO=9 and TS1='2016-12-13'";
		execute(sql);
		try {
			System.out.println(">>>executeAllOperations");
			g.executeAllOperations();
			 assertEquals(4, getRowCount(targetSchema + "." + targetTable));
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
		// change data in staging (older date -> will not end up in target)
		sql = "update " + sourceSchema + "." + sourceTable + " set "
				+ "SALARY=9999997, TS1='2016-12-13 12:13' where EMPNO=9 and TS1='2016-12-13'";
		execute(sql);
		try {
			System.out.println(">>>executeAllOperations");
			g.executeAllOperations();
			 assertEquals(4, getRowCount(targetSchema + "." + targetTable));
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
		// change data in staging: duplicate key and timestamp
		sql = "update " + sourceSchema + "." + sourceTable + " set "
				+ "SALARY=9999997, TS1='2016-12-13 12:13' where EMPNO=9";
		execute(sql);
		try {
			System.out.println(">>>executeAllOperations");
			g.executeAllOperations();
			fail();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	static void createTable(String sourceSchema, String sourceTable) {
		String sql = "create table " + sourceSchema + "." + sourceTable
				+ " (EMPNO DECIMAL(30,0) NOT NULL, FIRSTNAME VARCHAR(15) NOT NULL, BIRTHDATE DATE, SALARY DECIMAL(9,2), TS1 TIMESTAMP)";
		execute(sql);
		sql = "insert into " + sourceSchema + "." + sourceTable + " values "
				+ "(2,'Luke','1960-02-13',123456.78,'2016-12-13 12:34:50'),"
				+ "(5,'Obi-Wan','1922-12-15',5624534,'2016-12-13 12:34:51'),"
				+ "(9,'Yoda','1083-02-02',9999999,'2016-12-13'),"
				+ "(9,'Yoda','1083-01-01',9999998,'2016-12-12 12:34:52')";
		System.out.println(sql);
		execute(sql);
	}

}
