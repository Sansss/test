package com.golden.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.golden.model.HisValue;
import com.golden.model.StatisValue;

/**
 * ���������
 *
 */
public class CalculationEntity {
	/**
	 * ��ȡ����������ͳ��ֵ(���ֵ����Сֵ��ƽ��ֵ)
	 * 
	 * @param historyValue
	 *           �ֶ����ݼ��ϡ�
	 * @param pointerTime
	 * @return ͳ��ֵ�����
	 */
	public static StatisValue statisValue(List<HisValue> historyValue, long pointerTime) {
		StatisValue s = new StatisValue();
		List<Double> value = new ArrayList<Double>();
		double vs = 0;
		for (int i = 0; i < historyValue.size(); i++) {
			double v = historyValue.get(i).getValue();
			value.add(v);
			vs += v;
		}
		HisValue maxValue = new HisValue();
		HisValue minValue = new HisValue();
		HisValue avgValue = new HisValue();
		Collections.sort(value);
		Double max = value.get(historyValue.size() - 1);
		Double min = value.get(0);
		Double avg = vs / historyValue.size();
		maxValue.setValue(max);
		maxValue.setTime(pointerTime);
		minValue.setValue(min);
		minValue.setTime(pointerTime);
		avgValue.setValue(avg);
		avgValue.setTime(pointerTime);
		s.setMaxValue(maxValue);
		s.setMinValue(minValue);
		s.setAvgValue(avgValue);
		return s;
	}

}
