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

import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class EXASCDHelper extends AbstractEXASCDHelper {

	private String validTimePeriodStartColumn;
	private String validTimePeriodEndColumn;
	private String versionColumn;
	private String currentFlagColumn;
	private String statusInTempTableColumn = "scd_status";
	private String timeOfLastSCD3Change;

	private int versionStartsWith = 1;
	private String validFromDefaultValue = "'" + (new Timestamp(System.currentTimeMillis())).toString() + "'";

	private boolean createTargetTable = false;
	private boolean buildTargetTableWithPk = false;
	private boolean buildTargetTableWithSk = false;
	private boolean useCurrentFlag = false;
	private boolean useVersion = false;
	private String sKeyColumn;

	// returns
	private int countSCD2InsertNewRecords = 0;
	private int countSCD1ChangedRecords = 0;
	private int countSCD2ChangedRecords = 0;
	private int countSCD3ChangedRecords = 0;
	private int countOutdatedRecords = 0;

	public EXASCDHelper() {
		initFormatter();
	}

	boolean executeCreateTargetTable() {
		try {
			if (existsTargetTable()) {
				return true;
			}
			String createTableStatement = createTableStatement(getTargetSchemaTable(), true);
			logStatement(createTableStatement, "create target table");
			if (!doNotExecute) {
				executeStatement(createTableStatement);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	boolean executeCreateTempTable() {
		try {
			if (isEmpty(tempSchema)) {
				tempSchema = targetSchema;
			}
			while (!doNotExecute && existsTable(tempSchema, tempTable)) {
				tempTable = "temp_talend_" + ("" + Math.random()).substring(2, 7);
			}
			String createTableStatement = createTableStatement(getTempSchemaTable(), false);
			logStatement(createTableStatement, "create temp table");
			if (!doNotExecute) {
				executeStatement(createTableStatement);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	boolean executeDropTempTable() throws Exception {
		try {
			String dropTableStatement = "drop table " + getTempSchemaTable();
			logStatement(dropTableStatement, "drop temp table");
			if (!doNotExecute) {
				executeStatement(dropTableStatement);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * MAIN METHOD
	 * 
	 * @return
	 * @throws Exception
	 */
	public boolean executeAllOperations() throws Exception {
		if (!useInternalConnection && !isEmpty(sourceSchemaOverwrite)) {
			sourceSchema = sourceSchemaOverwrite;
		}

		try {
			checkState();
		} catch (IllegalStateException e) {
			throw e;
		}
		boolean errorsInExecution = false;
		String errorMessage = "Error occurred";
		boolean b = true;

		checkConnection();
		connection.setAutoCommit(false);

		// make sure that no duplicates are moved to temp table
		int maxDuplicate = getMaxDuplicatesInSource(false);
		if (maxDuplicate < 0) {
			errorsInExecution = true;
			errorMessage = "error accessing source";
			if (debug) {
				System.out.println("--" + errorMessage);
			}
		}
		if (maxDuplicate == 0) {
			if (debug) {
				System.out.println("--no data in source");
			}
			connection.setAutoCommit(nonInternalConnectionOriginalAutoCommit);
			return true;
		}
		if (maxDuplicate > 1) {
			if (isEmpty(timeStampInSourceColumn)) {
				errorsInExecution = true;
				errorMessage = "inconsistent state of source: duplicate key entries without provided timestamp";
				if (debug) {
					System.out.println("--" + errorMessage);
				}
			} else {
				int maxDuplicateWithTS = getMaxDuplicatesInSource(true);
				if (maxDuplicateWithTS > 1) {
					errorsInExecution = true;
					errorMessage = "inconsistent state of source: duplicate key entries without provided timestamp or with identical timestamp";
					if (debug) {
						System.out.println("--" + errorMessage);
					}
				}
			}

		}

		if (errorsInExecution) {
			if (nonInternalConnectionOriginalAutoCommit) {
				connection.setAutoCommit(nonInternalConnectionOriginalAutoCommit);
			}
			throw new Exception(errorMessage);
		}

		// create target table
		if (createTargetTable) {
			b = executeCreateTargetTable();
			if (!b) {
				errorsInExecution = true;
				errorMessage = "error creating target table";
				if (debug) {
					System.out.println("--" + errorMessage);
				}
			}
		}
		if (errorsInExecution) {
			connection.rollback();
			if (nonInternalConnectionOriginalAutoCommit) {
				connection.setAutoCommit(nonInternalConnectionOriginalAutoCommit);
			}
			throw new Exception(errorMessage);
		}

		// create temp table
		b = executeCreateTempTable();
		if (!b) {
			errorsInExecution = true;
			errorMessage = "error creating temp table";
			if (debug) {
				System.out.println("--" + errorMessage);
			}
		}
		if (errorsInExecution) {
			connection.rollback();
			if (nonInternalConnectionOriginalAutoCommit) {
				connection.setAutoCommit(nonInternalConnectionOriginalAutoCommit);
			}
			throw new Exception(errorMessage);
		}

		// move to temp table
		for (int rn = 1; rn <= maxDuplicate; rn++) {
			Statement statement = connection.createStatement();
			
			//executeBatch doesn't throw sqlexceptions for current jdbc driver
			List<String> batchWorkaroundList = new ArrayList<>();

			if (maxDuplicate == 1) {
				String sql = createMoveToTempTableStatement();
//				statement.addBatch(sql);
				batchWorkaroundList.add(sql);
				
				logStatement(sql, "move all data to temp table");
				if (debug) {
					System.out.println("--moving data to temp table");
					System.out.println(sql + ";");
				}
			} else {
				String sql = createMoveToTempTableStatement(rn);
//				statement.addBatch(sql);
				batchWorkaroundList.add(sql);
				
				logStatement(sql, "move data from partition " + rn + " of " + maxDuplicate + " to temp table");
				if (debug) {
					System.out.println("--moving data part " + rn + " of " + maxDuplicate + " to temp table");
					System.out.println(sql + ";");
				}
			}

			// create list of scd statements
			List<String> list = createAllSCDStatements(getTempSchemaTable(), getTargetSchemaTable());
			for (String sql : list) {
				if (isEmpty(sql)) {
					if (debug) {
						System.out.println("--executeSCDOperations statement: empty");
					}
				} else {
//					statement.addBatch(sql);
					batchWorkaroundList.add(sql);
					
					logStatement(sql, "scd statements");
				}
			}
			if (maxDuplicate > 1 && rn != maxDuplicate) {
				String sql = "truncate table " + getTempSchemaTable();
//				statement.addBatch(sql);
				batchWorkaroundList.add(sql);
				
				logStatement(sql, "truncate temp table");
				if (debug) {
					System.out.println("--truncate temp table");
					System.out.println(sql + ";");
				}
			}

			// execute if (!doNotExecute)
			if (!doNotExecute) {
//				int[] statReturnArray = statement.executeBatch();
				int[] statReturnArray = new int[batchWorkaroundList.size()];
				for (int i = 0; i < batchWorkaroundList.size(); i++) {
					statReturnArray[i] = statement.executeUpdate(batchWorkaroundList.get(i));
				}

				// check for errors
				for (int k = 0; k < statReturnArray.length; k++) {
					int batchRC = statReturnArray[k];
					if (batchRC < 0) {
						errorsInExecution = true;
						errorMessage = "Error during batch execution:";
						if (k == 0) {
							errorMessage += "Move to temp table";
						} else {
							String s = list.get(k + 1);
							errorMessage += s;
						}
					}
					if (debug) {
						System.out.println("--executeSCDOperations return: " + batchRC);
					}
				}

				if (!errorsInExecution) {
					// add counters
					int index = 1;
					if (enabledSCD2Versioning && containsSCDType("scd2")) {
						index = 7;
						int counter = statReturnArray[1];
						if (counter != statReturnArray[4] || counter != statReturnArray[5]) {
							errorsInExecution = true;
							errorMessage += "executeSCDOperations illegal batch counter result: 1, 4, 5";
							if (debug) {
								System.out.println("--executeSCDOperations illegal batch counter result: 1, 4, 5");
							}
						}
						countSCD2ChangedRecords += counter;

						counter = statReturnArray[2];
						countOutdatedRecords += counter;

						counter = statReturnArray[3];
						if (counter != statReturnArray[6]) {
							errorsInExecution = true;
							errorMessage += "executeSCDOperations illegal batch counter result: 3, 6";
							if (debug) {
								System.out.println("--executeSCDOperations illegal batch counter result: 3, 6");
							}
						}
						countSCD2InsertNewRecords += counter;
					}
					if (containsSCDType("scd1a")) {
						int counter = statReturnArray[index];
						countSCD1ChangedRecords += counter;
						index++;
					}
					if (containsSCDType("scd1b")) {
						int counter = statReturnArray[index];
						countSCD1ChangedRecords += counter;
						index++;
					}
					if (!enabledSCD2Versioning && !containsSCDType("scd1")) {
						int counter = statReturnArray[index];
						countSCD1ChangedRecords += counter;
						index++;
					}
					if (containsSCDType("scd3")) {
						int counter = statReturnArray[index];
						countSCD3ChangedRecords += counter;
						index++;
					}
				}
				if (statement != null) {
					try {
						statement.close();
					} catch (Exception d) {
					}
				}
			} // end of !doNotExecute

		} // all scd operations done

		executeDropTempTable();

		// commit or rollback
		if (!errorsInExecution) {
			if (debug) {
				System.out.println("--executeSCDOperations counter: " + countSCD2InsertNewRecords + ", "
						+ countSCD1ChangedRecords + "," + countSCD2ChangedRecords + "," + countSCD3ChangedRecords + ", "
						+ countOutdatedRecords);
			}
			if (!doNotExecute && (useInternalConnection || nonInternalConnectionOriginalAutoCommit)) {
				connection.commit();
				if (debug) {
					System.out.println("--executeSCDOperations commit");
				}
			}
			if (!useInternalConnection) {
				if (debug) {
					System.out.println("--executeSCDOperations set external connection autocommit="
							+ nonInternalConnectionOriginalAutoCommit);
				}
				connection.setAutoCommit(nonInternalConnectionOriginalAutoCommit);
			}
			return true;
		} else { // error
			if (!doNotExecute && (useInternalConnection || nonInternalConnectionOriginalAutoCommit)) {
				if (debug) {
					System.out.println("--executeSCDOperations rollback");
				}
				connection.rollback();
			}
			if (!useInternalConnection) {
				if (debug) {
					System.out.println("--executeSCDOperations set external connection autocommit="
							+ nonInternalConnectionOriginalAutoCommit);
				}
				connection.setAutoCommit(nonInternalConnectionOriginalAutoCommit);
			}
			throw new Exception(errorMessage);
		}
	}

	public void setValidTimePeriodStartColumn(String name) {
		validTimePeriodStartColumn = name;
	}

	public void setValidTimePeriodEndColumn(String name) {
		validTimePeriodEndColumn = name;
	}

	public void setTimeOfLastSCD3ChangeColumn(String name) {
		timeOfLastSCD3Change = name;
	}

	protected boolean isNotInAdditionalColumns(String name) {
		String s = name.toLowerCase();
		if (s.equals(validTimePeriodStartColumn.toLowerCase()) || s.equals(validTimePeriodEndColumn.toLowerCase())) {
			return false;
		}
		return true;
	}

	public Column addAdditionalColumn(String name, String type, Integer length, Integer precision, boolean nullable,
			boolean forInsert, boolean forUpdate) {
		if (isEmpty(name)) {
			throw new IllegalArgumentException("name cannot be null or empty");
		}
		if (isEmpty(type)) {
			throw new IllegalArgumentException("type cannot be null or empty");
		}
		Column col = new Column();
		col.setNotInSource(true);
		// col.setPreparedParam(true);
		col.setName(name);
		col.setDbType(type);
		col.setLength(length);
		col.setPrecision(precision);
		// col.setForInsert(forInsert);
		// col.setForUpdate(forUpdate);
		int pos = listSourceFields.indexOf(col);
		if (pos != -1) {
			listSourceFields.remove(pos);
		}
		listSourceFields.add(col);
		return col;
	}

	/*
	 * use timestampcolumn for move to temp table
	 */
	private boolean moveToTempTableUseTS() {
		boolean useTimestampInSource = false;
		if (!isEmpty(timeStampInSourceColumn) && !timeStampInSourceColumn.equals("null")
				&& (containsSCDType("scd2") || containsSCDType("scd3"))) {
			useTimestampInSource = true;
		}
		return useTimestampInSource;
	}

	private String moveToTempTableStart() {
		boolean useTimestampInSource = moveToTempTableUseTS();
		String sql = "insert into " + getTempSchemaTable() + " (";
		if (useTimestampInSource) {
			if (enabledSCD2Versioning) {
				sql += validTimePeriodStartColumn + ", ";
			} else if (!isEmpty(timeOfLastSCD3Change) && containsSCDType("scd3")) {
				sql += timeOfLastSCD3Change + ", ";
			} else {
				// scd0, scd1: no need to include this column
			}
		}
		return sql;
	}

	/*
	 * Move all data to temp table. When used it is called only once during processing.
	 */
	String createMoveToTempTableStatement() {
		boolean useTimestampInSource = moveToTempTableUseTS();
		String sql = moveToTempTableStart();
		sql += getCslSourceString(false) + ")\n";
		sql += "select ";
		if (useTimestampInSource) {
			sql += "coalesce(" + timeStampInSourceColumn + ", " + validFromDefaultValue + "),";
		}
		sql += getCslSourceString(false) + " from " + getSourceSchemaTable();
		if (!isEmpty(sourceWhereCondition)) {
			sql += " where " + sourceWhereCondition;
		}
		return sql;
	}

	/*
	 * Used when key duplicates exist. When used it is called multiple times during processing.
	 */
	String createMoveToTempTableStatement(int rn) {
		boolean useTimestampInSource = moveToTempTableUseTS();
		String sql = moveToTempTableStart();
		sql += getCslSourceString(false) + ")\n";
		sql += "select ";
		if (useTimestampInSource) {
			sql += "coalesce(" + timeStampInSourceColumn + ", " + validFromDefaultValue + "),";
		}
		sql += getCslSourceString(false) + "\n";
		sql += "from (\n";
		sql += "SELECT ";
		if (useTimestampInSource) {
			sql += timeStampInSourceColumn + ", ";
		}
		sql += getCslSourceString(false) + ", ROW_NUMBER() OVER (PARTITION BY ";
		sql += getCslSourceString(true, false, false, false, false);
		sql += " ORDER BY " + timeStampInSourceColumn + " ASC) AS RN\n";
		sql += "FROM " + getSourceSchemaTable() + ") t\n";
		sql += "where RN=" + rn;
		if (!isEmpty(sourceWhereCondition)) {
			sql += " and ( " + sourceWhereCondition + ")";
		}
		return sql;
	}

	/*
	 * parameter isTarget = true if create target table and false for temp table
	 */
	String createTableStatement(String schemaTable, boolean isTarget) {
		if (listSourceFields.isEmpty()) {
			throw new IllegalStateException("Source column list is empty!");
		}
		StringBuilder sb = new StringBuilder();
		sb.append("create table " + schemaTable + " (\n");

		if (isTarget && buildTargetTableWithSk) {
			sb.append(INDENT); // 4
			sb.append(sKeyColumn + " DECIMAL(36,0) IDENTITY" + ",\n");
		}

		if (enabledSCD2Versioning) {
			// create valid time period columns
			sb.append(INDENT); // 4
			sb.append(validTimePeriodStartColumn + " TIMESTAMP");
			if (!isTarget) {
				sb.append(" DEFAULT " + validFromDefaultValue);
			}
			sb.append(",\n" + INDENT); // 4
			sb.append(validTimePeriodEndColumn + " TIMESTAMP");
			sb.append(",\n" + INDENT); // 4
			// create version column
			if (useVersion) {
				sb.append(versionColumn + " DECIMAL(10,0) DEFAULT " + versionStartsWith + ",\n");
			}
			// create currentFlagColumn column
			if (useCurrentFlag) {
				sb.append(currentFlagColumn + " BOOLEAN" + ",\n");
			}
		} else { // SCD 2 disabled
			if (containsSCDType("scd3")) {
				sb.append(timeOfLastSCD3Change + " TIMESTAMP");
				if (!isTarget) {
					sb.append(" DEFAULT " + validFromDefaultValue);
				}
				sb.append(",\n");
			}
		}

		// source columns
		boolean firstLoop = true;
		for (Column col : listSourceFields) {
			if (!col.isNotInSource()) {
				if (firstLoop) {
					firstLoop = false;
				} else {
					sb.append(",\n");
				}
				sb.append(INDENT); // 4
				if (isTarget) {
					sb.append(createColumnCode(col));
				} else {
					sb.append(createColumnCode(col, false));
				}
			} else {
				if (isTarget) {
					if (firstLoop) {
						firstLoop = false;
					} else {
						sb.append(",\n");
					}
					sb.append(INDENT); // 4
					sb.append(createColumnCode(col));
				}
			}
		}
		// add status column for temp-table
		if (!isTarget) {
			while (isColumnInTargetSchema(statusInTempTableColumn)) {
				statusInTempTableColumn = "scd_status_" + ("" + Math.random()).substring(2, 7);
			}
			sb.append(",\n" + statusInTempTableColumn + " VARCHAR(10)" + " DEFAULT ' '");
		}
		// add pk (for scd 2 validTimePeriodStartColumn is included)
		if (isTarget && buildTargetTableWithPk) {
			sb.append(",\nprimary key(" + getCslSourceString(true, false, false, false, false));
			if (enabledSCD2Versioning) {
				sb.append(" ," + validTimePeriodStartColumn);
			}
			sb.append(")");
		}

		// close the field and constraint list
		sb.append("\n)");
		String sql = sb.toString();
		if (debug) {
			if (isTarget) {
				System.out.println("-- create target table");
			} else {
				System.out.println("-- create temp table");
			}
			System.out.println(sql + ";");
		}
		return sql;
	}

	private String createColumnCode(Column column) {
		String s = "";
		if (column.isScd3()) {
			s = createColumnCode(column, true) + ", ";
		}
		s += createColumnCode(column, false);
		return s;
	}

	private String createColumnCode(Column column, boolean additionalSCD3Col) {
		StringBuilder sb = new StringBuilder();
		if (!additionalSCD3Col) {
			sb.append(column.getName());
		} else {
			sb.append(column.getAdditionalSCD3Column());
		}
		sb.append(" ");
		String type = column.getDbType();
		if (type.contains("decimal")) {
			// add length and precision
			sb.append(type);
			sb.append("(");
			sb.append(column.getLength());
			sb.append(",");
			sb.append(column.getPrecision());
			sb.append(")");
		} else if (type.contains("char")) {
			// add only length
			sb.append(type);
			sb.append("(");
			sb.append(column.getLength());
			sb.append(")");
		} else
			// if (type.contains("boolean") || type.contains("int") ||
			// type.contains("double")
			// || type.contains("float") || type.contains("timestamp") ||
			// type.contains("date"))
			// {
			// add no length or precision
			sb.append(type);
		// }
		// else {
		// throw new RuntimeException(">>>EXASCDHelper unknown type: " + type);
		// }
		if (column.isNotNullable()) {
			sb.append(" NOT NULL");
		}
		return sb.toString();
	}

	List<String> createAllSCDStatements(String source, String target) {
		boolean mergeNeeded = true;

		List<String> list = new ArrayList<>();
		if (enabledSCD2Versioning && containsSCDType("scd2")) {
			mergeNeeded = false;
			list.add(createSCD2Statement1(source, target));
			list.add(createSCD2Statement1Old(source, target));
			list.add(createSCD2Statement2(source, target));
			list.add(createSCD2Statement3a(source, target));
			list.add(createSCD2Statement3b(source, target));
			list.add(createSCD2Statement3c(source, target));
		}
		if (containsSCDType("scd1a")) {
			mergeNeeded = false;
			list.add(createSCD1Statement(source, target, true));
		}
		if (containsSCDType("scd1b")) {
			mergeNeeded = false;
			list.add(createSCD1Statement(source, target, false));
		}
		if (mergeNeeded) {// when no scd 1 or scd 2 fields exist
			list.add(createSimpleInsertStatement(source, target));
		}
		if (containsSCDType("scd3")) {
			list.add(createSCD3Statement(source, target));
		}
		return list;
	}

	/*
	 * 
	 */
	String createSimpleInsertStatement(String source, String target) {
		StringBuilder sb = new StringBuilder();
		sb.append("merge into " + target + " ta");
		sb.append(" using (select " + getCslSourceString(false) + " from " + source);
		sb.append(") sr\n");
		sb.append(" on " + getKeyClause("sr", "ta") + "\n");
		sb.append(" when not matched then insert (" + getCslTargetString(false) + ")");
		sb.append(" values(" + getCslSourceString(true) + ")");
		String sql = sb.toString();
		if (debug) {
			System.out.println("-- createSimpleInsertStatement");
			System.out.println(sql + ";");
		}
		return sql;
	}

	/*
	 * Create statement for SCD type 1 data processing. If (and only if) SCD 2 is enabled the additional status column
	 * is used.
	 */
	String createSCD1Statement(String source, String target, boolean currentOnly) {
		if (!containsSCDType("scd1")) {
			return null;
		}
		if (enabledSCD2Versioning && containsSCDType("scd2")) {
			// with versioning enabled
			String scd1Type = currentOnly ? "scd1a" : "scd1b";
			StringBuilder sb = new StringBuilder();
			sb.append("merge into " + target + " ta using (\n");
			sb.append("SELECT " + validTimePeriodStartColumn + ", ");
			if (currentOnly) {
				sb.append(getCslSourceString(true, true, false, false, false) + "\n");
			} else {
				sb.append(getCslSourceString(true, false, true, false, false) + "\n");

			}
			sb.append("FROM " + source);
			if (currentOnly) {
				sb.append(" WHERE (" + statusInTempTableColumn + " != 'CHANGED' and " + statusInTempTableColumn
						+ " != 'NEW' and " + statusInTempTableColumn + " != 'OLD')");
			} else {
				sb.append(" WHERE (" + statusInTempTableColumn + " != 'NEW' and " + statusInTempTableColumn
						+ " != 'OLD')");
			}
			sb.append(") sr\n");
			sb.append("on " + getKeyClause("sr", "ta"));
			if (currentOnly) {
				sb.append(" and ta." + validTimePeriodEndColumn + "=" + scdEndDateStr);
			}
			sb.append(" when matched then update set " + getSetValuesClause("ta", "sr", scd1Type));
			sb.append(" where (" + getCompareColumnsClause("ta", "sr", scd1Type) + ")");
			String sql = sb.toString();
			if (debug) {
				System.out.println("-- createSCD1Statement");
				System.out.println(sql + ";");
			}
			return sql;
		} else {
			// no versioning (simple upsert)
			StringBuilder sb = new StringBuilder();
			sb.append("merge into " + target + " ta");
			sb.append(" using (select " + getCslSourceString(false) + " from " + source);
			sb.append(") sr\n");
			sb.append(" on " + getKeyClause("sr", "ta") + "\n");
			sb.append(" when not matched then insert (" + getCslTargetString(false) + ")");
			sb.append(" values(" + getCslSourceString(true) + ")");
			sb.append(" when matched then update set " + getSetValuesClause("ta", "sr", "scd1"));
			sb.append(" where " + getCompareColumnsClause("ta", "sr", "scd1"));
			String sql = sb.toString();
			if (debug) {
				System.out.println("-- createSCD1Statement");
				System.out.println(sql + ";");
			}
			return sql;
		}
	}

	/*
	 * SCD2 STEP 1a) Mark all changed entries in STAGE
	 */
	String createSCD2Statement1(String source, String target) {
		StringBuilder sb = new StringBuilder();
		sb.append("merge into " + source + " ta using (\n");
		sb.append("SELECT t.* FROM " + source + " s\n");
		sb.append("JOIN " + target + " t USING (\n");
		sb.append(getCslSourceString(true, false, false, false, false) + ")\n");
		sb.append("WHERE t." + validTimePeriodEndColumn + "=" + scdEndDateStr);
		sb.append(" AND (" + getCompareColumnsClause("s", "t", "scd2") + ")");
		sb.append(") sr\n");
		sb.append("on " + getKeyClause("sr", "ta") + "\n");
		sb.append("when matched then update set ta." + statusInTempTableColumn + "='CHANGED'");
		if (useVersion) {
			sb.append(", ta." + versionColumn + "=sr." + versionColumn);
		}
		sb.append(", ta." + validTimePeriodEndColumn + "=sr." + validTimePeriodEndColumn + "\n");
		// new condition for outdated updates
		sb.append("where ta." + validTimePeriodStartColumn + " > sr." + validTimePeriodStartColumn);

		String sql = sb.toString();
		if (debug) {
			System.out.println("-- createSCD2Statement1");
			System.out.println(sql + ";");
		}
		return sql;
	}

	/*
	 * SCD2 STEP 1b) Mark old entries in STAGE
	 */
	String createSCD2Statement1Old(String source, String target) {
		StringBuilder sb = new StringBuilder();
		sb.append("merge into " + source + " ta using (\n");
		sb.append("SELECT t.* FROM " + source + " s\n");
		sb.append("JOIN " + target + " t USING (\n");
		sb.append(getCslSourceString(true, false, false, false, false) + ")\n");
		sb.append("WHERE t." + validTimePeriodEndColumn + "=" + scdEndDateStr);
		sb.append(") sr\n");
		sb.append("on " + getKeyClause("sr", "ta") + "\n");
		sb.append("when matched then update set ta." + statusInTempTableColumn + "='OLD'\n");
		sb.append("where ta." + validTimePeriodStartColumn + " <= sr." + validTimePeriodStartColumn);

		String sql = sb.toString();
		if (debug) {
			System.out.println("-- createSCD2Statement1Old");
			System.out.println(sql + ";");
		}
		return sql;
	}

	/*
	 * SCD2 STEP 2) Mark all new entries in STAGE
	 */
	String createSCD2Statement2(String source, String target) {
		StringBuilder sb = new StringBuilder();
		sb.append("merge into " + source + " ta using (\n");
		sb.append("SELECT " + getCslSourceString(true, false, false, false, false) + "\n");
		sb.append("FROM " + source);
		sb.append(" WHERE NOT EXISTS ( \n");
		sb.append("select 1 from " + target + "\n");
		sb.append("where " + getKeyClause(source, target));
		sb.append("\n)) sr\n");
		sb.append("on " + getKeyClause("sr", "ta") + "\n");
		sb.append("when matched then update set " + statusInTempTableColumn + " = 'NEW'");
		String sql = sb.toString();
		if (debug) {
			System.out.println("-- createSCD2Statement2");
			System.out.println(sql + ";");
		}
		return sql;
	}

	/*
	 * SCD2 STEP 3) STAGE -> TARGET a) close old version (SCD2)
	 */
	String createSCD2Statement3a(String source, String target) {
		StringBuilder sb = new StringBuilder();
		sb.append("merge into " + target + " ta");
		sb.append(" using (SELECT *");
		sb.append(" FROM " + source);
		sb.append(" WHERE " + statusInTempTableColumn + " = 'CHANGED') sr\n");
		sb.append(" on " + getKeyClause("sr", "ta"));
		if (useVersion) {
			sb.append(" and ta." + versionColumn + "=sr." + versionColumn);
		}
		sb.append(" and ta." + validTimePeriodEndColumn + "=" + scdEndDateStr);
		sb.append(
				"\nwhen matched then update set ta." + validTimePeriodEndColumn + "=sr." + validTimePeriodStartColumn);
		if (useCurrentFlag) {
			sb.append(", ta." + currentFlagColumn + "=" + false);
		}

		String sql = sb.toString();
		if (debug) {
			System.out.println("-- createSCD2Statement3a");
			System.out.println(sql + ";");
		}
		return sql;
	}

	/*
	 * SCD2 STEP 3) STAGE -> TARGET b) insert new version (SCD2)
	 */
	String createSCD2Statement3b(String source, String target) {
		StringBuilder sb = new StringBuilder();
		sb.append("insert into " + target + " (");
		sb.append(validTimePeriodStartColumn + ", " + validTimePeriodEndColumn + ", ");
		if (useVersion) {
			sb.append(versionColumn + ",");
		}
		if (useCurrentFlag) {
			sb.append(currentFlagColumn + ",");
		}
		sb.append(getCslTargetString(false) + ")\n");
		sb.append("SELECT ");
		sb.append(validTimePeriodStartColumn + ", " + scdEndDateStr + ", ");
		if (useVersion) {
			sb.append(versionColumn + " +1, ");
		}
		if (useCurrentFlag) {
			sb.append(" true, ");
		}
		sb.append(getCslSourceString(true));
		sb.append(" FROM " + source + "\n");
		sb.append("WHERE " + statusInTempTableColumn + " = 'CHANGED'");
		String sql = sb.toString();
		if (debug) {
			System.out.println("-- createSCD2Statement3b");
			System.out.println(sql + ";");
		}
		return sql;
	}

	/*
	 * SCD2 STEP 3) STAGE -> TARGET c) insert completely new data.
	 */
	String createSCD2Statement3c(String source, String target) {
		StringBuilder sb = new StringBuilder();
		sb.append("insert into " + target + " (");
		sb.append(validTimePeriodStartColumn + ", " + validTimePeriodEndColumn + ", ");
		if (useVersion) {
			sb.append(versionColumn + ",");
		}
		if (useCurrentFlag) {
			sb.append(currentFlagColumn + ",");
		}
		sb.append(getCslTargetString(false) + ")\n");
		sb.append("SELECT ");
		sb.append(validTimePeriodStartColumn + ", " + scdEndDateStr + ", ");
		if (useVersion) {
			sb.append(versionStartsWith + ", ");
		}
		if (useCurrentFlag) {
			sb.append("true, ");
		}
		sb.append(getCslSourceString(true));
		sb.append(" FROM " + source + "\n");
		sb.append("WHERE " + statusInTempTableColumn + " = 'NEW'");
		String sql = sb.toString();
		if (debug) {
			System.out.println("-- createSCD2Statement3c");
			System.out.println(sql + ";");
		}
		return sql;
	}

	String createSCD3Statement(String source, String target) {
		boolean containsSCD3 = false;
		for (Column col : listSourceFields) {
			if (col.isScd3()) {
				containsSCD3 = true;
				break;
			}
		}
		if (!containsSCD3) {
			return null;
		}
		boolean updateTime = !enabledSCD2Versioning;

		StringBuilder sb = new StringBuilder();
		sb.append("merge into " + target + " ta using (\n");
		sb.append("SELECT ");
		if (updateTime) {
			sb.append(timeOfLastSCD3Change + ",");
		}
		sb.append(getCslSourceString(true, false, false, false, true));
		sb.append(" FROM " + source);
		if (enabledSCD2Versioning) {
			sb.append(" WHERE " + statusInTempTableColumn + "!='OLD'");
		}
		sb.append(") sr\n");
		sb.append("on (" + getKeyClause("ta", "sr") + ")\n");
		sb.append("when matched then update set ");
		if (updateTime) {
			sb.append("ta." + timeOfLastSCD3Change + "=sr." + timeOfLastSCD3Change + ", ");
		}
		sb.append(getSetValuesClauseSCD3("ta", "sr") + "\n");
		sb.append("where (" + getCompareColumnsClauseSCD3("ta", "sr") + ")");
		if (updateTime) {
			sb.append(" and (ta." + timeOfLastSCD3Change + " is null ");
			sb.append(" or ta." + timeOfLastSCD3Change + "<sr." + timeOfLastSCD3Change + ")");
		}

		String sql = sb.toString();
		if (debug) {
			System.out.println("-- createSCD3Statement");
			System.out.println(sql + ";");
		}
		return sql;
	}

	public boolean isBuildTargetTableWithPk() {
		return buildTargetTableWithPk;
	}

	public void setBuildTargetTableWithPk(boolean buildTargetTableWithPk) {
		this.buildTargetTableWithPk = buildTargetTableWithPk;
	}

	public void setBuildTargetTableWithSk(boolean b) {
		buildTargetTableWithSk = b;
	}

	public void setSKeyColumn(String sKeyColumn) {
		this.sKeyColumn = sKeyColumn;
	}

	public void setVersionColumn(String versionColumn) {
		this.versionColumn = versionColumn;
	}

	public void setVersionStartsWith(int versionStartsWith) {
		this.versionStartsWith = versionStartsWith;
	}

	public void setVersionEnabled(boolean useVersion) {
		this.useVersion = useVersion;
	}

	public void setCurrentFlagColumn(String currentFlagColumn) {
		this.currentFlagColumn = currentFlagColumn;
	}

	public void setUseCurrentFlag(boolean useCurrentFlag) {
		this.useCurrentFlag = useCurrentFlag;
	}

	boolean isColumnInTargetSchema(String name) {
		if (isEmpty(name)) {
			throw new IllegalArgumentException("column name must not be empty");
		}
		for (Column col : listSourceFields) {
			if (col.getName().trim().equals(name.trim())) {
				return true;
			}
			if (!isEmpty(col.getAdditionalSCD3Column()) && col.getAdditionalSCD3Column().trim().equals(name.trim())) {
				return true;
			}
		}
		if (!isEmpty(validTimePeriodStartColumn) && validTimePeriodStartColumn.trim().equals(name.trim())) {
			return true;
		}
		if (!isEmpty(validTimePeriodEndColumn) && validTimePeriodEndColumn.trim().equals(name.trim())) {
			return true;
		}
		if (!isEmpty(sKeyColumn) && sKeyColumn.trim().equals(name.trim())) {
			return true;
		}
		if (useVersion && !isEmpty(versionColumn) && versionColumn.trim().equals(name.trim())) {
			return true;
		}
		if (useCurrentFlag && !isEmpty(currentFlagColumn) && currentFlagColumn.trim().equals(name.trim())) {
			return true;
		}
		if (!isEmpty(timeOfLastSCD3Change) && timeOfLastSCD3Change.trim().equals(name.trim())) {
			return true;
		}
		return false;
	}

	public void setCreateTargetTable(boolean createTargetTable) {
		this.createTargetTable = createTargetTable;
	}

	public int[] getReturnCounters() {
		int[] a = { countSCD2InsertNewRecords, countSCD1ChangedRecords, countSCD2ChangedRecords,
				countSCD3ChangedRecords, countOutdatedRecords };
		return a;
	}

	void checkState() throws IllegalStateException {
		if (listSourceFields.isEmpty()) {
			throw new IllegalStateException("No source columns defined");
		}
		if (createTargetTable && buildTargetTableWithSk) {
			if (isEmpty(sKeyColumn)) {
				throw new IllegalStateException(
						"createTargetTable and buildTargetTableWithSk are enabled but no column for surrogate key specified");
			}
		}
		// check key
		String s = getCslSourceString(true, false, false, false, false);
		if (isEmpty(s)) {
			throw new IllegalStateException("No key column specified");
		}
		// check scd 2 options
		if (enabledSCD2Versioning) {
			if (isEmpty(validTimePeriodStartColumn) || isEmpty(validTimePeriodEndColumn)) {
				throw new IllegalStateException("For SCD 2 use timeperiod columns must be specified");
			}
		}
		if (useVersion) {
			if (isEmpty(versionColumn)) {
				throw new IllegalStateException("Use version is selected but no version column specified");
			}
		}
		if (useCurrentFlag) {
			if (isEmpty(currentFlagColumn)) {
				throw new IllegalStateException("Use current flag is selected but no current flag column specified");
			}
		}
		// check scd3
		for (Column col : listSourceFields) {
			if (col.isScd3()) {
				s = col.getAdditionalSCD3Column();
				if (isEmpty(s)) {
					throw new IllegalStateException("Missing additional scd 3 column for " + col.getName());
				}
			}
		}
		if (!enabledSCD2Versioning && containsSCDType("scd3")) {
			if (isEmpty(timeOfLastSCD3Change)) {
				throw new IllegalStateException("For SCD 3 use time change column must be specified");
			}
		}
	}

	public void setValidFromDefaultValue(String timestamp) {
		if (!isEmpty(timestamp)) {
			String s = timestamp.trim();
			if (!s.startsWith("'")) {
				s = "'" + s;
			}
			if (!s.endsWith("'")) {
				s = s + "'";
			}
			this.validFromDefaultValue = s;
		}
	}
}