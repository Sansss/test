package com.golden.model;

import java.io.Serializable;

import org.apache.kudu.Type;
import org.apache.kudu.client.RowResult;

/**
 * 历史数据类实体
 * 
 *
 */
public class HisValue implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * 标签点全名称
	 */

	private String PointName;
	/**
	 * 设备 ID （这里指风机设备）
	 */
	private String id;
	/**
	 * 时间戳（单位：毫秒）
	 */
	private Long time;
	/**
	 * 标签点类型
	 */
	private Type type;
	/**
	 * 浮点型存储值位
	 */
	private double value;
	/**
	 * 整型值存储位
	 * 
	 */
	private long state;

	/**
	 * 获取设备ID
	 * 
	 * @return 设备ID
	 */
	public String getId() {
		return id.trim();
	}

	/**
	 * 设置设备ID
	 * 
	 * @param id
	 *            设备ID
	 */
	public void setId(String id) {
		this.id = id.trim();
	}

	/**
	 * 获取时间 （单位毫秒）
	 * 
	 * @return 时间（毫秒值）
	 */
	public Long getTime() {
		return time;
	}

	/**
	 * 设置时间（单位毫秒）
	 * 
	 * @param time
	 *            时间（毫秒值）
	 */
	public void setTime(Long time) {
		this.time = time;
	}

	/**
	 * 获取标签点的类型
	 * 
	 * @return 标签点的类型
	 */
	public Type getType() {
		return type;
	}

	/**
	 * 设置标签点类型
	 * 
	 * @param type
	 *            标签点类型（设置时采用kudu规定的数据类型，为枚举类型：Type)
	 */
	public void setType(Type type) {
		this.type = type;
	}

	/**
	 * 获取浮点类型数据
	 * 
	 * @return 浮点类型数据
	 */
	public double getValue() {
		return value;
	}

	/**
	 * 设置浮点类型数据）
	 * 
	 * @param value
	 *            浮点类型数据
	 * 
	 */
	public void setValue(double value) {
		this.value = value;
	}

	/**
	 * 获取整型类数据
	 * 
	 * @return 整型类数据
	 */
	public long getState() {
		return state;
	}

	/**
	 * 设置整型类数据
	 * 
	 * @param state
	 *            整型类数据
	 */
	public void setState(long state) {
		this.state = state;
	}

	/**
	 * 获取标签点名称
	 * 
	 * @return 标签点名称
	 */
	public String getPointName() {
		return PointName.trim();
	}

	/**
	 * 设备标签点名称
	 * 
	 * @param pointName
	 *            标签点名称
	 */
	public void setPointName(String pointName) {
		PointName = pointName.trim();
	}

	/**
	 * 解析设置的方法(该方法为内部方法)
	 * 
	 * @param coumnType
	 *            列数据类型
	 * @param columnName
	 *            列名称
	 * @param result
	 *            kudu返回值对象
	 * @param resultVal
	 *            返回值对象
	 */
	public void parseType(String coumnType, String columnName, RowResult result, HisValue resultVal) {
		switch (coumnType) {
		case "double":
			double dvalue = result.getDouble(columnName);
			resultVal.setValue(dvalue);
			resultVal.setType(Type.DOUBLE);
			break;
		case "float":
			float fvalue = result.getFloat(columnName);
			resultVal.setValue(fvalue);
			resultVal.setType(Type.FLOAT);
			break;
		case "bool":
			boolean bvalue = result.getBoolean(columnName);
			resultVal.setState(bvalue == true ? 1 : 0);
			resultVal.setType(Type.BOOL);
			break;
		case "int16":
			short short1 = result.getShort(columnName);
			resultVal.setState(short1);
			resultVal.setType(Type.INT16);
			break;
		case "int32":
			int int1 = result.getInt(columnName);
			resultVal.setState(int1);
			resultVal.setType(Type.INT32);
			break;
		case "int64":
			int int2 = result.getInt(columnName);
			resultVal.setState(int2);
			resultVal.setType(Type.INT64);
			break;
		case "string":
			String svalue = result.getString(columnName);
			resultVal.setValue(Double.parseDouble(svalue));
			resultVal.setType(Type.STRING);
			break;

		default:
			break;
		}

	}
}
