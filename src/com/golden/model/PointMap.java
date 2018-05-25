package com.golden.model;

/**
 * 映射表实体类
 *
 */
public class PointMap {
	/**
	 * 列名称
	 */
	private String columnName;
	/**
	 * 表名称
	 */
	private String tableName;
	/**
	 * 标签点 ID
	 */
	private String id;

	/**
	 * 获取列名
	 * 
	 * @return 列名称
	 */
	public String getColumnName() {
		return columnName;
	}

	/**
	 * 设置列名称
	 * 
	 * @param columnName
	 *            列名称
	 */
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	/**
	 * 获取表名称
	 * 
	 * @return 表名称
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * 设置表名称
	 * 
	 * @param tableName
	 *            表名称
	 */
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	/**
	 * 设置标签点 ID ，即设备ID
	 * 
	 * @return 设备ID
	 */
	public String getId() {
		return id;
	}

	/**
	 * 设置标签点 ID ，即设备ID
	 * 
	 * @param id
	 *            设备ID
	 */
	public void setId(String id) {
		this.id = id;
	}

}
