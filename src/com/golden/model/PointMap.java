package com.golden.model;

/**
 * ӳ���ʵ����
 *
 */
public class PointMap {
	/**
	 * ������
	 */
	private String columnName;
	/**
	 * ������
	 */
	private String tableName;
	/**
	 * ��ǩ�� ID
	 */
	private String id;

	/**
	 * ��ȡ����
	 * 
	 * @return ������
	 */
	public String getColumnName() {
		return columnName;
	}

	/**
	 * ����������
	 * 
	 * @param columnName
	 *            ������
	 */
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	/**
	 * ��ȡ������
	 * 
	 * @return ������
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * ���ñ�����
	 * 
	 * @param tableName
	 *            ������
	 */
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	/**
	 * ���ñ�ǩ�� ID �����豸ID
	 * 
	 * @return �豸ID
	 */
	public String getId() {
		return id;
	}

	/**
	 * ���ñ�ǩ�� ID �����豸ID
	 * 
	 * @param id
	 *            �豸ID
	 */
	public void setId(String id) {
		this.id = id;
	}

}
