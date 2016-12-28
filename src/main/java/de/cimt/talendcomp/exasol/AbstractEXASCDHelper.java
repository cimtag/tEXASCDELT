/**
 * Copyright 2016 cimt objects ag
 * Jan Lolling jan.lolling@gmail.com
 * Frederik Demuth frederik.demuth@cimt-ag.de
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.cimt.talendcomp.exasol;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
// import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public abstract class AbstractEXASCDHelper {

	protected Connection connection;
	protected boolean useInternalConnection = true;
	protected boolean nonInternalConnectionOriginalAutoCommit = true;
	protected boolean debug = false;
	protected String sourceTable;
	protected String sourceSchema;
	protected String sourceWhereCondition;
	protected String targetTable;
	protected String targetSchema;
	protected String tempTable = "temp_talend_" + ("" + Math.random()).substring(2, 7);
	protected String tempSchema;
	private boolean enableLogStatements = false;
	private String statementsLogFilePath = null;
	private File statementLogFile = null;
	private String logSectionComent = null;
	protected boolean doNotExecute = false;
	protected String scdEndDateStr = "'9999-01-01'";
	protected static final String INDENT = "    ";
	protected List<String> preparedParamList = new ArrayList<String>();
	protected Map<String, Object> paramValues = new HashMap<String, Object>();
	protected List<Column> listSourceFields = new ArrayList<Column>();
	protected SimpleDateFormat sdfDate = null;
	protected SimpleDateFormat sdfTimestamp = null;
	protected NumberFormat nf = null;
	protected boolean enabledSCD2Versioning = true;

	protected void initFormatter() {
		nf = NumberFormat.getInstance(Locale.ENGLISH);
		nf.setGroupingUsed(false);
		sdfDate = new SimpleDateFormat("yyyy-MM-dd");
		sdfTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	}

	public void connect(String host, String port, String database, String user, String password,
			String propertiesStr) throws Exception {
		if (isEmpty(host)) {
			throw new IllegalArgumentException("host cannot be null or empty");
		}
		if (isEmpty(port)) {
			throw new IllegalArgumentException("port cannot be null or empty");
		}
		if (isEmpty(database)) {
			throw new IllegalArgumentException("database cannot be null or empty");
		}
		if (isEmpty(user)) {
			throw new IllegalArgumentException("user cannot be null or empty");
		}
		if (isEmpty(password)) {
			throw new IllegalArgumentException("password cannot be null or empty");
		}
		StringBuilder url = new StringBuilder();
		Class.forName("com.exasol.jdbc.EXADriver");

		url.append("jdbc:exa:");
		url.append(host + ":" + port);
		url.append(";schema=" + sourceSchema);
		connection = DriverManager.getConnection(url.toString(), user, password);
		checkConnection();
		useInternalConnection = true;
	}

	public static boolean isEmpty(String s) {
		return s == null || s.trim().isEmpty();
	}

	protected void checkConnection() throws Exception {
		if (connection == null) {
			throw new Exception("Connection is not created");
		}
		if (connection.isClosed()) {
			throw new Exception("Connection is already closed");
		}
		if (connection.isReadOnly()) {
			throw new Exception("Connection is read only");
		}
	}

	public void setConnection(Connection connection) throws SQLException {
		if (connection != null) {
			useInternalConnection = false;
			this.connection = connection;
			nonInternalConnectionOriginalAutoCommit = connection.getAutoCommit();
		}
	}

	public void commit() throws Exception {
		connection.commit();
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}

	public void setSourceTable(String sourceTable) {
		if (isEmpty(sourceTable)) {
			throw new IllegalArgumentException("source table name cannot be empty or null");
		}
		this.sourceTable = sourceTable;
	}

	public String getSourceWhereCondition() {
		return sourceWhereCondition;
	}

	public void setSourceWhereCondition(String sourceWhereCondition) {
		if (!isEmpty(sourceWhereCondition)) {
			sourceWhereCondition = sourceWhereCondition.trim();
			if (sourceWhereCondition.toLowerCase().startsWith("where")) {
				sourceWhereCondition = sourceWhereCondition.substring("where".length());
			}
			this.sourceWhereCondition = sourceWhereCondition;
		}
	}

	public void setSourceSchema(String sourceSchema) {
		if (isEmpty(sourceSchema)) {
			throw new IllegalArgumentException("source schema name cannot be empty or null");
		}
		this.sourceSchema = sourceSchema;
	}

	public void setTargetTable(String targetTable) {
		if (isEmpty(targetTable)) {
			throw new IllegalArgumentException("target table name cannot be empty or null");
		}
		this.targetTable = targetTable;
	}

	public void setTargetSchema(String targetSchema) {
		if (isEmpty(targetSchema)) {
			throw new IllegalArgumentException("target schema name cannot be empty or null");
		}
		this.targetSchema = targetSchema;
	}

	public void setTempTable(String tempTable) {
		if (isEmpty(tempTable)) {
			throw new IllegalArgumentException("temp table name cannot be empty or null");
		}
		this.tempTable = tempTable;
	}

	public void setTempSchema(String tempSchema) {
		if (isEmpty(tempSchema)) {
			throw new IllegalArgumentException("temp schema name cannot be empty or null");
		}
		this.tempSchema = tempSchema;
	}

	protected String getTargetSchemaTable() {
		return targetSchema + "." + targetTable;
	}

	protected String getSourceSchemaTable() {
		return sourceSchema + "." + sourceTable;
	}

	protected String getTempSchemaTable() {
		StringBuilder sb = new StringBuilder();
		if (isEmpty(tempSchema) == false) {
			sb.append(tempSchema);
			sb.append(".");
		} else if (isEmpty(targetSchema) == false) {
			sb.append(targetSchema);
			sb.append(".");
		}
		sb.append(tempTable);
		return sb.toString();
	}

	public boolean existsTable(String schema, String table) throws Exception {
		if (isEmpty(schema) || isEmpty(table)) {
			throw new IllegalArgumentException("existsTable schema and table name must not be empty");
		}
		if (doNotExecute) {
			return false;
		}
		checkConnection();
		DatabaseMetaData dbmd = connection.getMetaData();
		ResultSet rs = dbmd.getTables(null, schema.toUpperCase(), table.toUpperCase(), null);
		boolean exists = false;
		if (rs != null && rs.next()) {
			exists = true;
		}
		rs.close();
		return exists;
	}

	public boolean existsTargetTable() throws Exception {
		return existsTable(targetSchema, targetTable);
	}

	/*
	 * execute statement if (!doNotExecute). No commit, but rollback on error
	 */
	protected void executeStatement(String statement) throws Exception {
		checkConnection();
		Statement stat = connection.createStatement();
		try {
			// logging done in other methods
			// logStatement(statement, null);
			if (doNotExecute == false) {
				stat.execute(statement);
			}
		} catch (Exception e) {
			if (useInternalConnection) {
				connection.rollback();
			}
			throw e;
		} finally {
			if (stat != null) {
				try {
					stat.close();
				} catch (Exception d) {
				}
			}
		}
	}

	protected void logStatement(String statement, String comment) {
		if (enableLogStatements && statementsLogFilePath != null) {
			boolean newLogSection = false;
			if (statementLogFile == null) {
				statementLogFile = new File(statementsLogFilePath);
				newLogSection = true;
			}
			File dir = statementLogFile.getParentFile();
			if (dir != null && dir.exists() == false) {
				dir.mkdirs();
			}
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			try {
				BufferedWriter bw = new BufferedWriter(
						new OutputStreamWriter(new FileOutputStream(statementLogFile, true), "UTF-8"));
				if (newLogSection) {
					if (logSectionComent != null) {
						bw.write("-- ########### " + logSectionComent + " #########\n");
						bw.write("-- ########### run at " + sdf.format(new Date()) + " ######### \n\n");
					} else {
						bw.write("-- ########### run at " + sdf.format(new Date()) + " ######### \n\n");
					}
				}
				if (comment != null && comment.isEmpty() == false) {
					bw.write("-- " + comment + "\n");
				}
				bw.write(statement);
				bw.write(";\n\n");
				bw.flush();
				bw.close();
			} catch (Exception e) {
				System.err.println(e.getMessage());
			}
		}
	}

	public boolean isEnableLogStatements() {
		return enableLogStatements;
	}

	public void setEnableLogStatements(boolean enableLogStatements) {
		this.enableLogStatements = enableLogStatements;
	}

	public String getStatementsLogFile() {
		return statementsLogFilePath;
	}

	public void setStatementsLogFile(String statementsLogFile, String comment) {
		if (isEmpty(statementsLogFile) == false) {
			this.statementsLogFilePath = statementsLogFile;
		}
		if (isEmpty(comment) == false) {
			this.logSectionComent = comment.replace("\n", " ");
		}
	}

	public boolean isDoNotExecuteMode() {
		return doNotExecute;
	}

	public void doNotExecuteMode(boolean readOnly) {
		this.doNotExecute = readOnly;
	}

	public void addSourceColumn(String name, String type, Integer length, Integer precision,
			boolean nullable, boolean isKey, boolean scd1a, boolean scd1b, boolean scd2, boolean scd3,
			String additionalSCD3Column) {
		Column col = new Column();
		if (isEmpty(name)) {
			throw new IllegalArgumentException("name cannot be null or empty");
		}
		if (isEmpty(type)) {
			throw new IllegalArgumentException("type cannot be null or empty");
		}
		col.setName(name);
		col.setDbType(type);
		col.setLength(length);
		col.setPrecision(precision);
		col.setNotNullable(nullable == false);
		col.setPartOfSourceKey(isKey);
		col.setScd1a(scd1a);
		col.setScd1b(scd1b);
		col.setScd2(scd2);
		col.setScd3(scd3);
		col.setAdditionalSCD3Column(additionalSCD3Column);
		int pos = listSourceFields.indexOf(col);
		if (pos != -1) {
			listSourceFields.remove(pos);
		}
		listSourceFields.add(col);
	}

	protected String toSQLString(Object o) {
		if (o instanceof Timestamp) {
			return "'" + sdfTimestamp.format((Timestamp) o) + "'";
		} else if (o instanceof Date) {
			return "'" + sdfDate.format((Date) o) + "'";
		} else if (o instanceof Number) {
			return nf.format((Number) o);
		} else if (o instanceof String) {
			return "'" + ((String) o) + "'";
		} else if (o instanceof Boolean) {
			return ((Boolean) o) ? "true" : "false";
		} else if (o != null) {
			return o.toString();
		} else {
			return "null";
		}
	}

	public void setScdEndDate(String scdEndDateStr) {
		if (!isEmpty(scdEndDateStr)) {
			String s = scdEndDateStr.trim();
			if (!s.startsWith("'")) {
				s = "'" + s;
			}
			if (!s.endsWith("'")) {
				s = s + "'";
			}
			this.scdEndDateStr = s;
		}
	}

	public void setScdEndDate(Date scdEndDate) {
		if (scdEndDate != null) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			this.scdEndDateStr = "'" + sdf.format(scdEndDate) + "'";
		}
	}

	/**
	 * Return comma separated names of all source columns
	 * e.g. COLUMN1, COLUMN2, COLUMN3
	 * 
	 * @return
	 */
	public String getCslSourceString(boolean includeAdditionalSCD3Column) {
		String s = "";
		boolean first = true;
		for (Column col : listSourceFields) {
			if (!col.isNotInSource()) {
				if (!first) {
					s += ", ";
				}
				if (includeAdditionalSCD3Column) {
					if (col.isScd3()) {
						s += col.getName() + ", ";
					}
				}
				s += col.getName();
				first = false;
			}
		}
		return s;
	}

	/**
	 * Return comma separated names of target columns incl. additional columns for SCD 3 (but
	 * excl. SCD 2 versioning). Set param true to include other additional "not in source" columns.
	 * e.g. COLUMN1, COLUMN2, COLUMN3
	 * 
	 * @return
	 */
	public String getCslTargetString(boolean inclAddCols) {
		String s = "";
		boolean first = true;
		for (Column col : listSourceFields) {
			if (inclAddCols || !col.isNotInSource()) {
				if (!first) {
					s += ", ";
				}
				if (col.isScd3()) {
					s += col.getAdditionalSCD3Column() + ", ";
				}
				s += col.getName();
				first = false;
			}
		}
		return s;
	}

	/**
	 * Return comma separated names of all source columns matching one of the "true"-arguments
	 * e.g. COLUMN1, COLUMN2, COLUMN3
	 * 
	 * @param key
	 * @param scd1a
	 * @param scd1b
	 * @param scd2
	 * @param scd3
	 * @return
	 */
	public String getCslSourceString(boolean key, boolean scd1a, boolean scd1b, boolean scd2,
			boolean scd3) {
		String s = "";
		boolean first = true;
		for (Column col : listSourceFields) {
			if (!col.isNotInSource()) {
				if ((key && col.isPartOfSourceKey()) || (scd1a && col.isScd1a()) || (scd1b && col.isScd1b())
						|| (scd2 && col.isScd2()) || (scd3 && col.isScd3())) {
					if (!first) {
						s += ", ";
					}
					s += col.getName();
					first = false;
				}
			}
		}
		return s;
	}

	/**
	 * Create string to compare columns from two tables that match scdType.
	 * Example return:
	 * "s.FIRSTNAME!=t.FIRSTNAME or s.LASTNAME!=t.LASTNAME"
	 * If columns are nullable:
	 * 
	 * @param table1
	 * @param table2
	 * @param scdType
	 * @return
	 */
	public String getCompareColumnsClause(String table1, String table2, String scdType) {
		String s = "";
		boolean first = true;
		for (Column col : listSourceFields) {

			if (!col.isNotInSource()) {
				if (col.isScd(scdType)) {
					if (!first) {
						s += " or ";
					}
					if (col.isNotNullable()) {
						s += table1 + "." + col.getName() + "!=" + table2 + "." + col.getName();
					} else {
						s += "coalesce(" + table1 + "." + col.getName() + "," + col.getNVL() + ")!=";
						s += "coalesce(" + table2 + "." + col.getName() + "," + col.getNVL() + ")";
					}
					first = false;
				}
			}
		}
		return s;
	}

	/**
	 * get a String like: t1.key1=t2.key1 and t1.key2=t2.key2
	 */
	public String getKeyClause(String table1, String table2) {
		String s = "";
		boolean first = true;
		for (Column col : listSourceFields) {

			if (!col.isNotInSource()) {
				if (col.isPartOfSourceKey()) {
					if (!first) {
						s += " and ";
					}
					s += table1 + "." + col.getName() + "=" + table2 + "." + col.getName();
					first = false;
				}
			}
		}
		return s;
	}

	/**
	 * get a String like: t1.col1=t2.col1, t1.col2=t2.col2
	 */
	public String getSetValuesClause(String table1, String table2, String scdType) {
		String s = "";
		boolean first = true;
		for (Column col : listSourceFields) {
			if (!col.isNotInSource()) {
				if (col.isScd(scdType)) {
					if (!first) {
						s += ", ";
					}
					s += table1 + "." + col.getName() + "=" + table2 + "." + col.getName();
					first = false;
				}
			}
		}
		return s;
	}

	public String getSetValuesClauseSCD3(String table1, String table2) {
		String s = "";
		boolean first = true;
		for (Column col : listSourceFields) {
			if (!col.isNotInSource() && col.isScd3()) {
				if (!first) {
					s += ", ";
				}
				s += table1 + "." + col.getAdditionalSCD3Column() + "=" + table2 + "." + col.getName();
				first = false;
			}
		}
		return s;
	}

	/**
	 * Like getCompareColumnsClause but only for SCD type 3 and using additionalSCD3ColumnName for
	 * table1
	 * 
	 * @param table1
	 * @param table2
	 * @param op
	 * @return
	 */
	public String getCompareColumnsClauseSCD3(String table1, String table2) {
		String s = "";
		boolean first = true;
		for (Column col : listSourceFields) {
			if (!col.isNotInSource() && col.isScd3()) {
				if (!first) {
					s += " or ";
				}
				if (col.isNotNullable()) {
					s += table1 + "." + col.getAdditionalSCD3Column() + "!=" + table2 + "." + col.getName();
				} else {
					s += "coalesce(" + table1 + "." + col.getAdditionalSCD3Column() + "," + col.getNVL()
							+ ")!=";
					s += "coalesce(" + table2 + "." + col.getName() + "," + col.getNVL() + ")";
				}
				first = false;
			}
		}
		return s;
	}

	public boolean containsSCDType(String scdType) {
		for (Column col : listSourceFields) {
			if (col.isScd(scdType)) {
				return true;
			}
		}
		return false;
	}

	public void setEnableSCD2Versioning(boolean enableSCD2Versioning) {
		this.enabledSCD2Versioning = enableSCD2Versioning;
	}

	/*
	 * return -1 for error, 0 for no data, positive int as maximum duplicate count
	 */
	int getMaxDuplicatesInSource() {
		if (doNotExecute) {
			return 1;
		}
		String s = getCslSourceString(true, false, false, false, false);
		String sql = "select max(rn) from(SELECT ";
		sql += s;
		sql += ", count(*) AS rn FROM " + getSourceSchemaTable();
		sql += " group by " + s + ")";
		Statement stmt;
		try {
			stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			if (rs.next()) {
				return rs.getInt(1);
			} else {
				return 0;
			}
		} catch (SQLException e) {
			System.err.println("error accessing source");
			return -1;
		}
	}
}
