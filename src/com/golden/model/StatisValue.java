package com.golden.model;

import java.io.Serializable;

/**
 * ͳ����ʵ��
 *
 */
public class StatisValue implements Serializable {

	private static final long serialVersionUID = 1L;
	/**
	 * ���ֵ����
	 */
	private HisValue maxValue = new HisValue();
	/**
	 * ��Сֵ����
	 */
	private HisValue minValue = new HisValue();
	/**
	 * ƽ��ֵ����
	 */
	private HisValue avgValue = new HisValue();

	public HisValue getMaxValue() {
		return maxValue;
	}

	public void setMaxValue(HisValue maxValue) {
		this.maxValue = maxValue;
	}

	public HisValue getMinValue() {
		return minValue;
	}

	public void setMinValue(HisValue minValue) {
		this.minValue = minValue;
	}

	public HisValue getAvgValue() {
		return avgValue;
	}

	public void setAvgValue(HisValue avgValue) {
		this.avgValue = avgValue;
	}

}
