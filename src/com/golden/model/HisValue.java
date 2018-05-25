package com.golden.model;

import java.io.Serializable;

import org.apache.kudu.Type;
import org.apache.kudu.client.RowResult;

/**
 * ��ʷ������ʵ��
 * 
 *
 */
public class HisValue implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * ��ǩ��ȫ����
	 */

	private String PointName;
	/**
	 * �豸 ID ������ָ����豸��
	 */
	private String id;
	/**
	 * ʱ�������λ�����룩
	 */
	private Long time;
	/**
	 * ��ǩ������
	 */
	private Type type;
	/**
	 * �����ʹ洢ֵλ
	 */
	private double value;
	/**
	 * ����ֵ�洢λ
	 * 
	 */
	private long state;

	/**
	 * ��ȡ�豸ID
	 * 
	 * @return �豸ID
	 */
	public String getId() {
		return id.trim();
	}

	/**
	 * �����豸ID
	 * 
	 * @param id
	 *            �豸ID
	 */
	public void setId(String id) {
		this.id = id.trim();
	}

	/**
	 * ��ȡʱ�� ����λ���룩
	 * 
	 * @return ʱ�䣨����ֵ��
	 */
	public Long getTime() {
		return time;
	}

	/**
	 * ����ʱ�䣨��λ���룩
	 * 
	 * @param time
	 *            ʱ�䣨����ֵ��
	 */
	public void setTime(Long time) {
		this.time = time;
	}

	/**
	 * ��ȡ��ǩ�������
	 * 
	 * @return ��ǩ�������
	 */
	public Type getType() {
		return type;
	}

	/**
	 * ���ñ�ǩ������
	 * 
	 * @param type
	 *            ��ǩ�����ͣ�����ʱ����kudu�涨���������ͣ�Ϊö�����ͣ�Type)
	 */
	public void setType(Type type) {
		this.type = type;
	}

	/**
	 * ��ȡ������������
	 * 
	 * @return ������������
	 */
	public double getValue() {
		return value;
	}

	/**
	 * ���ø����������ݣ�
	 * 
	 * @param value
	 *            ������������
	 * 
	 */
	public void setValue(double value) {
		this.value = value;
	}

	/**
	 * ��ȡ����������
	 * 
	 * @return ����������
	 */
	public long getState() {
		return state;
	}

	/**
	 * ��������������
	 * 
	 * @param state
	 *            ����������
	 */
	public void setState(long state) {
		this.state = state;
	}

	/**
	 * ��ȡ��ǩ������
	 * 
	 * @return ��ǩ������
	 */
	public String getPointName() {
		return PointName.trim();
	}

	/**
	 * �豸��ǩ������
	 * 
	 * @param pointName
	 *            ��ǩ������
	 */
	public void setPointName(String pointName) {
		PointName = pointName.trim();
	}

	/**
	 * �������õķ���(�÷���Ϊ�ڲ�����)
	 * 
	 * @param coumnType
	 *            ����������
	 * @param columnName
	 *            ������
	 * @param result
	 *            kudu����ֵ����
	 * @param resultVal
	 *            ����ֵ����
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
