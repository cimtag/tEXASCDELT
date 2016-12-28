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

import java.util.HashMap;
import java.util.Map;

public class Column {

	private String name;
	private String dbType;
	private int length = 0;
	private int precision = 0;
	private String javaClass;
	private boolean notInSource = false;
	private boolean partOfSourceKey = false;
	// private boolean trackChanges = false;
	private boolean createIndex = false;
	private boolean notNullable = false;
	// private boolean preparedParam = false;
	private boolean autoIncrement = false;
	// private boolean forInsert = true;
	// private boolean forUpdate = true;
	private Map<String, Boolean> scdTypes = new HashMap<>();
	private String additionalSCD3Column;

	public String getNVL() {
		if (dbType.contains("char")) {
			return "' '";
		}
		if (dbType.contains("date") || dbType.contains("timestamp")) {
			return "'0001-01-01'";
		}
		if (dbType.contains("int") || dbType.contains("decimal")) {
			return "0";
		}
		if (dbType.contains("float") || dbType.contains("double")) {
			return "0.0";
		}
		if (dbType.contains("boolean")) {
			return "true";
		}
		return "' '";
	}

	public String getName() {
		return name;
	}

	public Column setName(String name) {
		if (name == null || name.isEmpty()) {
			throw new IllegalArgumentException("name cannot be null or empty");
		}
		this.name = name;
		return this;
	}

	public boolean isNotInSource() {
		return notInSource;
	}

	public void setNotInSource(boolean notInSource) {
		this.notInSource = notInSource;
	}

	public String getDbType() {
		return dbType;
	}

	public Column setDbType(String dbType) {
		this.dbType = dbType.toLowerCase().trim();
		return this;
	}

	public int getLength() {
		return length;
	}

	public Column setLength(Integer length) {
		if (length != null) {
			this.length = length;
		}
		return this;
	}

	public int getPrecision() {
		return precision;
	}

	public Column setPrecision(Integer precision) {
		if (precision != null) {
			this.precision = precision;
		}
		return this;
	}

	public String getJavaClass() {
		return javaClass;
	}

	public Column setJavaClass(String javaClass) {
		this.javaClass = javaClass;
		return this;
	}

	public boolean isPartOfSourceKey() {
		return partOfSourceKey;
	}

	public Column setPartOfSourceKey(Boolean partOfSourceKey) {
		if (partOfSourceKey != null) {
			this.partOfSourceKey = partOfSourceKey;
		}
		return this;
	}

	public boolean isCreateIndex() {
		return createIndex;
	}

	public Column setCreateIndex(Boolean createIndex) {
		if (createIndex != null) {
			this.createIndex = createIndex;
		}
		return this;
	}

	public boolean isNotNullable() {
		return notNullable || partOfSourceKey || autoIncrement;
	}

	public Column setNotNullable(Boolean notNullable) {
		if (notNullable != null) {
			this.notNullable = notNullable;
		}
		return this;
	}

	// public boolean isPreparedParam() {
	// return preparedParam;
	// }

	// public Column setPreparedParam(Boolean preparedParam) {
	// if (preparedParam != null) {
	// this.preparedParam = preparedParam;
	// }
	// return this;
	// }

	public boolean isAutoIncrement() {
		return autoIncrement;
	}

	public Column setAutoIncrement(Boolean autoIncrement) {
		if (autoIncrement != null) {
			this.autoIncrement = autoIncrement;
		}
		return this;
	}

	public boolean isScd(String type) {
		if (isPartOfSourceKey()) {
			return false;
		}
		if (isNotInSource()) {
			return false;
		}
		if (type.equals("scd1")) {
			return isScd1a() || isScd1b();
		}
		Boolean b = scdTypes.get(type);
		return b != null ? b : false;
	}

	public boolean isScd1a() {
		if (isPartOfSourceKey()) {
			return false;
		}
		if (isNotInSource()) {
			return false;
		}
		Boolean b = scdTypes.get("scd1a");
		return b != null ? b : false;
	}

	public void setScd1a(boolean b) {
		scdTypes.put("scd1a", b);
	}

	public boolean isScd1b() {
		if (isPartOfSourceKey()) {
			return false;
		}
		if (isNotInSource()) {
			return false;
		}
		Boolean b = scdTypes.get("scd1b");
		return b != null ? b : false;
	}

	public void setScd1b(boolean b) {
		scdTypes.put("scd1b", b);
	}

	public boolean isScd2() {
		if (isPartOfSourceKey()) {
			return false;
		}
		if (isNotInSource()) {
			return false;
		}
		Boolean b = scdTypes.get("scd2");
		return b != null ? b : false;
	}

	public void setScd2(boolean b) {
		scdTypes.put("scd2", b);
	}

	public boolean isScd3() {
		if (isPartOfSourceKey()) {
			return false;
		}
		if (isNotInSource()) {
			return false;
		}
		Boolean b = scdTypes.get("scd3");
		return b != null ? b : false;
	}

	public void setScd3(boolean b) {
		scdTypes.put("scd3", b);
	}

	public void setAdditionalSCD3Column(String additionalSCD3Column) {
		if (additionalSCD3Column != null && additionalSCD3Column.trim().isEmpty() == false) {
			this.additionalSCD3Column = additionalSCD3Column;
		}
	}

	/**
	 * @return additional scd 3 column if isScd3, otherwise null
	 */
	public String getAdditionalSCD3Column() {
		if (isScd3() && AbstractEXASCDHelper.isEmpty(additionalSCD3Column)) {
			throw new IllegalStateException("Illegal scd 3 state for column: " + name);
		} else if (isScd3()) {
			return additionalSCD3Column;
		} else {
			return null;
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null) {
			return false;
		}
		if (o instanceof Column) {
			return name.toLowerCase().equals(((Column) o).name.toLowerCase());
		} else if (o instanceof String) {
			return name.toLowerCase().equals(((String) o).toLowerCase());
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return name.hashCode();
	}

}
