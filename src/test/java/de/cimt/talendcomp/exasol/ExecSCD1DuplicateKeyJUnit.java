package de.cimt.talendcomp.exasol;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExecSCD1DuplicateKeyJUnit extends AbstractTestBase {

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
		// g.setTimestampInSourceColumn("ts");
		g.connect(host, port, schema, user, password, null);
		g.setSourceWhereCondition("1 = 1");
	}

	@Test
	public final void test1() {
		// g.addSourceColumn(name, type, length, precision, nullable, isKey, scd1a, scd1b, scd2, scd3,
		// additionalSCD3Column);
		g.addSourceColumn("ID", "DECIMAL", 30, 0, false, true, false, false, false, false, null);
		g.addSourceColumn("A", "VARCHAR", 30, 0, false, false, true, false, false, false, null);

		// With key duplicate and no TimestampInSourceColumn
		try {
			g.executeAllOperations();
			fail();
		} catch (Exception e) {
			if (!e.getMessage().contains("duplicate key entries without provided timestamp")) {
				fail();
			}
		}

		// With setTimestampInSourceColumn
		g.setTimestampInSourceColumn("ts");
		try {
			g.executeAllOperations();
		} catch (Exception e) {
			fail();
			e.printStackTrace();
		}

		// With duplicate for key and TimestampInSourceColumn
		String sql = "insert into " + sourceSchema + "." + sourceTable + " values (2,'a3','1960-02-14')";
		execute(sql);
		g.setTimestampInSourceColumn("ts");
		try {
			g.executeAllOperations();
			fail();
		} catch (Exception e) {
			if (!e.getMessage().contains("duplicate key timestamp entries")) {
				fail();
			}
		}
	}

	static void createTable(String sourceSchema, String sourceTable) {
		String sql = "create table " + sourceSchema + "." + sourceTable
				+ " (ID DECIMAL(30,0) NOT NULL, A VARCHAR(15) NOT NULL, ts DATE NOT NULL)";
		execute(sql);
		sql = "insert into " + sourceSchema + "." + sourceTable + " values (2,'a1','1960-02-13'),(2,'a2','1960-02-14')";
		execute(sql);
	}

}
