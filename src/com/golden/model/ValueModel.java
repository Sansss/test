package com.golden.model;

import org.apache.kudu.Type;

/**
 * 数据值模型，主 要用于传递数据的封装。
 *
 */
public class ValueModel {
	/**
	 * 全局标签点名称
	 */
	private String pointName;
	/**
	 * 设备编号
	 */
	private String id;
	/**
	 * 值类型
	 */
	private Type type;
	/**
	 * 值 ，该数据类型需根据该实例中 Type 字段进行相应转换。
	 */
	private Object value;
	/**
	 * 时间戳
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
