package com.golden.model;

import java.io.Serializable;

/**
 * 统计类实体
 *
 */
public class StatisValue implements Serializable {

	private static final long serialVersionUID = 1L;
	/**
	 * 最大值对象
	 */
	private HisValue maxValue = new HisValue();
	/**
	 * 最小值对象
	 */
	private HisValue minValue = new HisValue();
	/**
	 * 平均值对象
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
