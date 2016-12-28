package de.cimt.talendcomp.exasol;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

public class TestStringGenJUnit {

	static EXASCDHelper g;
	static String sourceSchema = "test_sr";
	static String sourceTable = "EMP_NEW";
	static String targetSchema = "test_tg";
	static String targetTable = "EMPLOYEE";

	@Before
	public void setUp() throws Exception {
		g = new EXASCDHelper();
		g.setDebug(false);
		g.setSourceSchema(sourceSchema);
		g.setSourceTable(sourceTable);
		g.setTargetSchema(targetSchema);
		g.setTargetTable(targetTable);
		g.setValidTimePeriodEndColumn("VALID_TO");
		g.setValidTimePeriodStartColumn("VALID_FROM");
	}

	static String getSource() {
		return sourceSchema + "." + sourceTable;
	}

	static String getTarget() {
		return targetSchema + "." + targetTable;
	}

	@Test
	public final void getCsl() {
		// g.addSourceColumn(name, type, length, precision, nullable, isKey, scd1a, scd1b, scd2, scd3,
		// additionalSCD3Column);
		g.addSourceColumn("EMPNO", "DECIMAL", 30, 0, false, true, false, false, false, false, null);
		assertEquals("EMPNO", g.getCslSourceString(true, false, false, false, false));
		assertEquals("EMPNO", g.getCslSourceString(false));
		assertEquals("EMPNO", g.getCslTargetString(false));
		assertEquals("EMPNO", g.getCslTargetString(true));
		assertEquals("", g.getCslSourceString(false, true, false, false, false));

		g.addSourceColumn("FIRSTNAME", "VARCHAR", 15, 0, false, true, true, true, false, false, null);
		assertEquals("EMPNO, FIRSTNAME", g.getCslSourceString(true, false, false, false, false));
		g.addSourceColumn("FIRSTNAME", "VARCHAR", 15, 0, false, false, true, true, false, false, null);
		assertEquals("FIRSTNAME", g.getCslSourceString(false, true, false, false, false));
		g.addSourceColumn("LASTNAME", "VARCHAR", 15, 0, false, false, true, true, false, true,
				"CURRENT_LASTNAME");
		assertEquals("FIRSTNAME, LASTNAME", g.getCslSourceString(false, true, false, false, false));
		assertEquals("EMPNO, FIRSTNAME, CURRENT_LASTNAME, LASTNAME", g.getCslTargetString(false));
		assertEquals("EMPNO, FIRSTNAME, CURRENT_LASTNAME, LASTNAME", g.getCslTargetString(true));

	}

//	@Test
//	public final void getCompareColumnsClause() {
//		// g.addSourceColumn(name, type, length, precision, nullable, isKey, trackChanges, scd1a, scd1b,
//		// scd2, scd3);
//		g.addSourceColumn("EMPNO", "DECIMAL", 30, 0, false, true, false, false, false, false, null);
//		assertEquals("", g.getCompareColumnsClause("src", "tgt", "=", ",", false, "scd1a"));
//		g.addSourceColumn("FIRSTNAME", "VARCHAR", 15, 0, true, false, true, true, false, false, null);
//		assertEquals("src.FIRSTNAME=tgt.FIRSTNAME",
//				g.getCompareColumnsClause("src", "tgt", "=", ",", false, "scd1a"));
//		g.addSourceColumn("LASTNAME", "VARCHAR", 15, 0, false, false, true, true, false, false, null);
//		assertEquals("src.FIRSTNAME=tgt.FIRSTNAME , src.LASTNAME=tgt.LASTNAME",
//				g.getCompareColumnsClause("src", "tgt", "=", ",", false, "scd1a"));
//		assertEquals("src.FIRSTNAME!=tgt.FIRSTNAME or src.LASTNAME!=tgt.LASTNAME",
//				g.getCompareColumnsClause("src", "tgt", "!=", "or", false, "scd1a"));
//	}

	/*
	 * create table (temp and target) is ok
	 */
	@Test
	public final void createTempTable() {
		// g.addSourceColumn(name, type, length, precision, nullable, isKey, scd1a, scd1b,
		// scd2, scd3);
		g.addSourceColumn("EMPNO", "DECIMAL", 30, 0, false, true, false, false, false, false, null);
		g.addSourceColumn("FIRSTNAME", "VARCHAR", 15, 0, true, false, true, true, false, false, null);
		g.addSourceColumn("LASTNAME", "VARCHAR", 15, 0, true, false, true, true, false, false, null);
		g.addSourceColumn("Birthday", "DATE", 10, 0, true, false, true, true, false, false, null);

		// g.setEnableSCD2Versioning(false);

		g.setVersionColumn("version");
		g.setVersionEnabled(false);

		g.setCurrentFlagColumn("cur");
		g.setUseCurrentFlag(true);

		String s = g.createTableStatement("test.temp", false);
//		assertEquals(
//				"create table test.temp (\n    VALID_FROM TIMESTAMP,\n    VALID_TO TIMESTAMP,\n    cur BOOLEAN,\n    EMPNO DECIMAL(30,0) NOT NULL,\n    FIRSTNAME VARCHAR(15),\n    LASTNAME VARCHAR(15),\n    Birthday DATE,\nscd_status VARCHAR(10)\n)",
//				s);
//		System.out.println(s);

	}

	@Test
	public final void createMoveToTempTable() {
		// g.addSourceColumn(name, type, length, precision, nullable, isKey, scd1a, scd1b,
		// scd2, scd3);
		g.addSourceColumn("EMPNO", "DECIMAL", 30, 0, false, true, false, false, true, false, null);
		g.addSourceColumn("FIRSTNAME", "VARCHAR", 15, 0, true, false, true, false, true, false, null);
		g.addSourceColumn("LASTNAME", "VARCHAR", 15, 0, true, false, true, false, true, false, null);
		
		g.setTimestampInSourceColumn("time_in_source");

		String s = g.createMoveToTempTableStatement();
		s = g.createMoveToTempTableStatement(1);
//		 System.out.println(s);
	}

	@Test
	public final void createSCD1aStatement() {
//		// g.addSourceColumn(name, type, length, precision, nullable, isKey, scd1a, scd1b, scd2, scd3,
//		// addtionalSCD3Column);
//		g.addSourceColumn("EMPNO", "DECIMAL", 30, 0, false, true, false, false, false, false, null);
//		String s = g.createSCD1Statement(getSource(), getTarget(), true);
//		assertEquals(null, s);
//
//		g.addSourceColumn("FIRSTNAME", "VARCHAR", 15, 0, true, false, true, true, false, false, null);
//		g.addSourceColumn("LASTNAME", "VARCHAR", 15, 0, false, false, true, true, false, false, null);
//
//		s = g.createSCD1Statement(getSource(), getTarget(), true);
//		assertEquals(
//				"merge into test_tg.EMPLOYEE ta using (SELECT EMPNO, FIRSTNAME, LASTNAME FROM test_sr.EMP_NEW WHERE (ELT_STATUS != 'CHANGED' "
//						+ "and ELT_STATUS != 'NEW')) sr on sr.EMPNO=ta.EMPNO and ta.VALID_TO='9999-12-30' when matched then update set ta.FIRSTNAME=sr.FIRSTNAME , "
//						+ "ta.LASTNAME=sr.LASTNAME where (ta.FIRSTNAME!=sr.FIRSTNAME and ta.LASTNAME!=sr.LASTNAME)",
//				s);
//		s = g.createSCD1Statement(getSource(), getTarget(), false);
//		assertEquals(
//				"merge into test_tg.EMPLOYEE ta using (SELECT EMPNO, FIRSTNAME, LASTNAME FROM test_sr.EMP_NEW WHERE (ELT_STATUS != 'CHANGED' "
//						+ "and ELT_STATUS != 'NEW')) sr on sr.EMPNO=ta.EMPNO when matched then update set ta.FIRSTNAME=sr.FIRSTNAME , ta.LASTNAME=sr.LASTNAME "
//						+ "where (ta.FIRSTNAME!=sr.FIRSTNAME and ta.LASTNAME!=sr.LASTNAME)",
//				s);
//		g.addSourceColumn("ADDRESS", "VARCHAR", 100, 0, true, false, true, true, false, false, null);
//		g.addSourceColumn("BIRTHDATE", "DATE", 15, 0, false, false, true, true, false, false, null);
//		g.addSourceColumn("REGION", "VARCHAR", 2, 0, true, false, true, true, false, false, null);
//		g.addSourceColumn("SALARY", "DECIMAL", 9, 2, false, false, true, true, false, false, null);
//		s = g.createSCD1Statement(getSource(), getTarget(), false);
//		assertEquals(
//				"merge into test_tg.EMPLOYEE ta using (SELECT EMPNO, FIRSTNAME, LASTNAME, ADDRESS, BIRTHDATE, REGION, SALARY FROM test_sr.EMP_NEW "
//						+ "WHERE (ELT_STATUS != 'CHANGED' and ELT_STATUS != 'NEW')) sr on sr.EMPNO=ta.EMPNO when matched then update set ta.FIRSTNAME=sr.FIRSTNAME , "
//						+ "ta.LASTNAME=sr.LASTNAME , ta.ADDRESS=sr.ADDRESS , ta.BIRTHDATE=sr.BIRTHDATE , ta.REGION=sr.REGION , ta.SALARY=sr.SALARY "
//						+ "where (ta.FIRSTNAME!=sr.FIRSTNAME and ta.LASTNAME!=sr.LASTNAME and ta.ADDRESS!=sr.ADDRESS and ta.BIRTHDATE!=sr.BIRTHDATE "
//						+ "and ta.REGION!=sr.REGION and ta.SALARY!=sr.SALARY)",
//				s);
//		g.addSourceColumn("FIRSTNAME", "VARCHAR", 15, 0, false, true, false, false, false, false, null);
//		s = g.createSCD1Statement(getSource(), getTarget(), false);
//		assertEquals(
//				"merge into test_tg.EMPLOYEE ta using (SELECT EMPNO, LASTNAME, ADDRESS, BIRTHDATE, REGION, SALARY, FIRSTNAME "
//						+ "FROM test_sr.EMP_NEW WHERE (ELT_STATUS != 'CHANGED' and ELT_STATUS != 'NEW')) sr on sr.EMPNO=ta.EMPNO and sr.FIRSTNAME=ta.FIRSTNAME "
//						+ "when matched then update set ta.LASTNAME=sr.LASTNAME , ta.ADDRESS=sr.ADDRESS , ta.BIRTHDATE=sr.BIRTHDATE , ta.REGION=sr.REGION , "
//						+ "ta.SALARY=sr.SALARY where (ta.LASTNAME!=sr.LASTNAME and ta.ADDRESS!=sr.ADDRESS and ta.BIRTHDATE!=sr.BIRTHDATE and ta.REGION!=sr.REGION "
//						+ "and ta.SALARY!=sr.SALARY)",
//				s);
//		// System.out.println(s);
	}

	// @Test
	// public final void createSCD1StatementNoSCD2() {
	// // g.addSourceColumn(name, type, length, precision, nullable, isKey, scd1a, scd1b,
	// // scd2, scd3);
	// g.setEnableSCD2Versioning(false);
	// g.addSourceColumn("EMPNO", "DECIMAL", 30, 0, false, true, false, false, false, false, null);
	// g.addSourceColumn("FIRSTNAME", "VARCHAR", 15, 0, true, false, true, true, false, false, null);
	// g.addSourceColumn("LASTNAME", "VARCHAR", 15, 0, false, false, true, true, false, false, null);
	//
	// String s = g.createSCD1Statement(getSource(),getTarget(),true);
	//
	// // System.out.println(s);
	// }

	@Test
	public final void createSCD2Statement() {
		// g.addSourceColumn(name, type, length, precision, nullable, isKey, scd1a, scd1b,
		// scd2, scd3);
		g.addSourceColumn("EMPNO", "DECIMAL", 30, 0, false, true, false, false, true, false, null);
		g.addSourceColumn("FIRSTNAME", "VARCHAR", 15, 0, true, false, true, false, true, false, null);
		g.addSourceColumn("LASTNAME", "VARCHAR", 15, 0, true, false, true, false, true, false, null);
		// g.setVersionColumn("VERSION");
		// assertEquals(
		// "merge into test_sr.EMP_NEW ta using (SELECT t.* FROM test_sr.EMP_NEW s "
		// + "JOIN test_tg.EMPLOYEE t USING (EMPNO) WHERE t.VALID_TO='9999-12-30' AND
		// (s.FIRSTNAME!=t.FIRSTNAME or s.LASTNAME!=t.LASTNAME)) sr "
		// + "on sr.EMPNO=ta.EMPNO when matched then update set ta.ELT_STATUS='CHANGED',
		// ta.VALID_FROM=sr.VALID_FROM, ta.VALID_TO=sr.VALID_TO",
		// g.createSCD2Statement1(getSource(), getTarget()));
		// assertEquals(
		// "merge into test_sr.EMP_NEW ta using (SELECT EMPNO FROM test_sr.EMP_NEW WHERE EMPNO NOT IN( "
		// + "select EMPNO from test_tg.EMPLOYEE)) sr on sr.EMPNO=ta.EMPNO when matched then update set
		// ELT_STATUS = 'NEW'",
		// g.createSCD2Statement2(getSource(), getTarget()));
		//
		// String s = g.createSCD2Statement3b(getSource(), getTarget());
		// System.out.println(s);
	}

}
