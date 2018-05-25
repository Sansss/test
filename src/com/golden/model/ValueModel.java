package com.golden.model;

import org.apache.kudu.Type;

/**
 * ����ֵģ�ͣ��� Ҫ���ڴ������ݵķ�װ��
 *
 */
public class ValueModel {
	/**
	 * ȫ�ֱ�ǩ������
	 */
	private String pointName;
	/**
	 * �豸���
	 */
	private String id;
	/**
	 * ֵ����
	 */
	private Type type;
	/**
	 * ֵ ����������������ݸ�ʵ���� Type �ֶν�����Ӧת����
	 */
	private Object value;
	/**
	 * ʱ���
	 */
	private long time;

	public String getPointName() {
		return pointName;
	}

	public void setPointName(String pointName) {
		this.pointName = pointName;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

}
