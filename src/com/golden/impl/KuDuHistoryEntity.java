package com.golden.impl;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kudu.Type;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduPredicate.ComparisonOp;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduScanner.KuduScannerBuilder;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.apache.kudu.client.Update;
import org.apache.kudu.client.Upsert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.golden.interfac.GoldenDataSource;
import com.golden.model.HisValue;
import com.golden.model.PointMap;
import com.golden.model.StatisValue;
import com.golden.model.ValueModel;
import com.golden.util.DateUtils;
import com.rtdb.api.model.ValueData;

/**
 * 
 *
 */
public class KuDuHistoryEntity implements GoldenDataSource {
	private static Logger logger = LoggerFactory.getLogger(KuDuHistoryEntity.class.getName());
	private static Map<String, String> mappingMap = null;
	private KuduClient client;
	private String mappingTableName;

	public KuDuHistoryEntity(String kuduMaster, String mappingTableName) {
		this.client = new KuduClient.KuduClientBuilder(kuduMaster).build();
		this.mappingTableName = mappingTableName;
		if (mappingMap == null) {
			mappingMap = new HashMap<String, String>();
			try {
				initMappingMap();
			} catch (KuduException e) {
				logger.info("��ȡӳ�������ʧ��.....");
				e.printStackTrace();
			}
		}
	}

	private void initMappingMap() throws KuduException {
		logger.debug("���ڼ���ӳ�����Ϣ....");
		KuduTable table = client.openTable(mappingTableName);
		KuduScannerBuilder builder = client.newScannerBuilder(table);
		KuduScanner scaner = builder.build();
		while (scaner.hasMoreRows()) {
			RowResultIterator iterator = scaner.nextRows();
			while (iterator.hasNext()) {
				RowResult rs = iterator.next();
				mappingMap.put(rs.getString("name"), rs.getString("kudu_table"));
			}
		}
		if (mappingMap == null) {
			logger.error("����ӳ�����Ϣʧ�ܣ�");
		}
		scaner.close();
	}

	/**
	 * ���ݱ�ǩ�����ƻ�ȡ������ʷ����
	 * 
	 * @param pointName
	 *            ��ǩ��ȫ����
	 * @return ��ʷ���ݵļ���
	 * @throws KuduException
	 * @throws Exception
	 * @throws IOException
	 * @throws UnknownHostException
	 */
	@Override
	public List<HisValue> getHistory(String pointName) throws KuduException {
		PointMap pm = arrangementPoint(pointName);
		String tableName = pm.getTableName();
		String columnName = pm.getColumnName();
		String id = pm.getId();
		KuduTable table = client.openTable(tableName);
		if ((table.getName() == null) || table.getName() == "") {
			logger.info("��������ȷ����ʧ��...");
		}

		List<String> projectColumns = new ArrayList<String>(3);
		projectColumns.add("id");
		projectColumns.add("time");
		projectColumns.add(columnName);
		KuduPredicate predicate = KuduPredicate.newComparisonPredicate(table.getSchema().getColumn("id"),
				ComparisonOp.EQUAL, id);
		KuduScanner scb = client.newScannerBuilder(table).setProjectedColumnNames(projectColumns)
				.addPredicate(predicate).build();
		List<HisValue> list = new ArrayList<HisValue>();
		// ��ȡ���е�����
		String columnType = table.getSchema().getColumn(columnName).getType().getName().trim();

		while (scb.hasMoreRows()) {
			RowResultIterator iterator = scb.nextRows();
			while (iterator.hasNext()) {
				HisValue hisValue = new HisValue();
				RowResult result = iterator.next();
				String resultId = result.getString("id");
				String time = result.getString("time");
				hisValue.setPointName(pointName.toUpperCase());

				hisValue.parseType(columnType, columnName, result, hisValue);

				hisValue.setTime(DateUtils.stringToDate(time).getTime());
				hisValue.setId(resultId);
				list.add(hisValue);
			}
		}
		scb.close();

		Collections.sort(list, new Comparator<HisValue>() {
			@Override
			public int compare(HisValue h1, HisValue h2) {
				return h1.getTime().compareTo(h2.getTime());
			}
		});
		return list;
	}

	/**
	 * ���ݱ�ǩ�����ƻ�ȡһ��ʱ���ڵ���ʷֵ
	 * 
	 * @param pointName
	 *            ��ǩ��ȫ����
	 * @param startTime
	 *            ��ʼʱ��
	 * @param endTime
	 *            ����ʱ��
	 * @return �������ݵļ���
	 * @throws KuduException
	 */
	@Override
	public List<HisValue> getHistory(String pointName, long startTime, long endTime) throws KuduException {
		PointMap pm = arrangementPoint(pointName);
		String tableName = pm.getTableName();
		String columnName = pm.getColumnName();
		String id = pm.getId();

		KuduTable table = client.openTable(tableName);
		if ((table.getName() == null) || table.getName() == "") {
			logger.info("��������ȷ����ʧ��...");
		}
		List<String> projectColumns = new ArrayList<String>(3);
		projectColumns.add("id");
		projectColumns.add("time");
		projectColumns.add(columnName);
		KuduPredicate p1 = KuduPredicate.newComparisonPredicate(table.getSchema().getColumn("id"), ComparisonOp.EQUAL,
				id);
		KuduPredicate p2 = null;
		if (startTime != 0l) {

			p2 = KuduPredicate.newComparisonPredicate(table.getSchema().getColumn("time"), ComparisonOp.GREATER_EQUAL,
					DateUtils.TimeStampDates(startTime));
		}
		KuduPredicate p3 = null;
		if (endTime != 0l) {

			p3 = KuduPredicate.newComparisonPredicate(table.getSchema().getColumn("time"), ComparisonOp.LESS_EQUAL,
					DateUtils.TimeStampDates(endTime));
		}
		KuduScannerBuilder builder = client.newScannerBuilder(table).setProjectedColumnNames(projectColumns)
				.addPredicate(p1);
		if (p2 != null) {
			builder.addPredicate(p2);
		}
		if (p3 != null) {
			builder.addPredicate(p3);
		}
		KuduScanner scanner = builder.build();
		List<HisValue> list = new ArrayList<HisValue>();
		// ��ȡ���е�����
		String columnType = table.getSchema().getColumn(columnName).getType().getName().trim();
		while (scanner.hasMoreRows()) {
			RowResultIterator iterator = scanner.nextRows();
			while (iterator.hasNext()) {
				HisValue resultVal = new HisValue();
				RowResult result = iterator.next();
				String ids = result.getString("id");
				String str = result.getString("time");
				long time = DateUtils.stringToDate(str).getTime();
				resultVal.setPointName(pointName);
				resultVal.parseType(columnType, columnName, result, resultVal);
				resultVal.setTime(time);
				resultVal.setId(ids);
				list.add(resultVal);
			}

		}

		scanner.close();

		Collections.sort(list, new Comparator<HisValue>() {
			@Override
			public int compare(HisValue h1, HisValue h2) {
				return h1.getTime().compareTo(h2.getTime());
			}
		});
		return list;

	}

	/**
	 * ���ݱ�ǩ���ȡָ��ʱ�����ʷֵ
	 * 
	 * @param pointName
	 *            ��ǩ��ȫ����
	 * @param time
	 *            ��ָ����ʱ��
	 * @return ��ʷ����
	 * @throws KuduException
	 */
	@Override
	public HisValue getHistoryInTime(String pointName, long time) throws KuduException {
		PointMap pm = arrangementPoint(pointName);
		String tableName = pm.getTableName();
		String columnName = pm.getColumnName();
		String id = pm.getId();
		KuduTable table = null;

		table = client.openTable(tableName);
		if ((table.getName() == null) || table.getName() == "") {
			logger.info("��������ȷ����ʧ��...");
		}

		List<String> projectColumns = new ArrayList<String>(3);
		projectColumns.add("id");
		projectColumns.add("time");
		projectColumns.add(columnName);
		String searchtime = DateUtils.longToString(time, "yyyy-MM-dd HH:mm:ss");
		KuduPredicate predicate1 = KuduPredicate.newComparisonPredicate(table.getSchema().getColumn("time"),
				ComparisonOp.EQUAL, searchtime);
		KuduPredicate predicate2 = KuduPredicate.newComparisonPredicate(table.getSchema().getColumn("id"),
				ComparisonOp.EQUAL, id);
		KuduScanner scanner = client.newScannerBuilder(table).addPredicate(predicate1).addPredicate(predicate2)
				.setProjectedColumnNames(projectColumns).build();
		HisValue resultVal = null;
		// ��ȡ���е�����
		String columnType = table.getSchema().getColumn(columnName).getType().getName().trim();
		while (scanner.hasMoreRows()) {
			for (RowResult rowResult : scanner.nextRows()) {
				resultVal = new HisValue();
				resultVal.setId(rowResult.getString("id"));
				resultVal.setTime(DateUtils.stringToDate(rowResult.getString("time")).getTime());
				resultVal.setPointName(pointName);
				resultVal.parseType(columnType, columnName, rowResult, resultVal);
			}

		}
		scanner.close();

		return resultVal;
	}

	/**
	 * ����ָ��������ȡָ����ǩ��ĵȼ��ֵ
	 * 
	 * @param pointName
	 *            ��ǩ��ȫ����
	 * @param startTime
	 *            ��ʼʱ��
	 * @param endTime
	 *            ����ʱ��
	 * @param intervalTime
	 *            ���ʱ�䣨��λ�����룩
	 * @return ������������ʷ���ݵļ���
	 * @throws KuduException
	 */
	@Override
	public List<HisValue> getIntervalHistory(String pointName, long startTime, long endTime, long intervalTime)
			throws KuduException {
		long vasTime = endTime - startTime;
		// �����ʵ�ʻ�ȡ�ĵȼ��ֵ�ĸ���
		long count = vasTime / intervalTime;
		// ��ȡ��ʱ��ε����е�����
		//long s = System.currentTimeMillis();
		List<HisValue> history = getHistory(pointName, startTime, endTime);
		//System.out.println(System.currentTimeMillis() - s);
		List<HisValue> list = new ArrayList<HisValue>();
		// ָ��ʱ��
		long pointerTime = startTime;
		long tempNum = 0;
		if (history != null) {
			// ��ȡ��ʵֵ���ϵĵ�һ��ֵ��ʱ��
			long fristTime = history.get(0).getTime();
			long lastTime = history.get(history.size() - 1).getTime();
			// ��ǰ��һ��ֵ��ʱ�䲻���ڿ�ʼʱ��
			if (fristTime > startTime && tempNum == 0) {
				// ȡ��һ��ֵ��
				HisValue upperValue = getUpperValue(pointName, pointerTime);
				// �����ǰ��Ҫ��������ֵ
				long num = (fristTime - startTime) / intervalTime;
				// ѭ����ֵ
				for (int j = 0; j <= num; j++) {
					HisValue h = new HisValue();
					h.setId(upperValue.getId());
					h.setPointName(upperValue.getPointName());
					h.setTime(pointerTime);
					h.setType(upperValue.getType());
					h.setValue(upperValue.getState());
					list.add(h);
					// ָ��ʱ����ƫ��
					pointerTime += intervalTime;
					// ����ֵ�ĸ���
					tempNum++;
				}
			}
			for (int i = 0; i < history.size(); i++) {
				// ��ȡ��ǰֵ��ʱ��
				long realTime = history.get(i).getTime();
				// �жϵ�ǰֵ���ǿ�ʼʱ���ֵ���������ֻ�е�ǰֵ��ʱ��С�ڿ�ʼʱ�䣬����Ҫ��ֵ��
				if (realTime != pointerTime) {

					// ��ǰֵ��ʱ�����ָ��ʱ�䣬��Ҫȡ��һ��ֵ������
					if (realTime > pointerTime ) {
						HisValue hv = history.get(i - 1);
						if (hv != null) {
							HisValue hist = new HisValue();
							hist.setId(hv.getId());
							hist.setTime(pointerTime);
							hist.setPointName(hv.getPointName());
							hist.setType(hv.getType());
							hist.setValue(hv.getValue());
							hist.setState(hv.getState());
							list.add(hist);
							// ָ��ʱ����ƫ��
							pointerTime += intervalTime;
							// ֵ�����ݼ���
							tempNum++;
							i--;

						}

					}

				} else if (realTime == pointerTime) {
					// ��ǰʱ����ָƫ��ʱ�䣬˵����ָ����ʱ����ϣ���������
					HisValue hv = history.get(i);
					if (hv != null) {
						HisValue hist = new HisValue();
						hist.setId(hv.getId());
						hist.setTime(pointerTime);
						hist.setPointName(hv.getPointName());
						hist.setType(hv.getType());
						hist.setValue(hv.getValue());
						hist.setState(hv.getState());
						list.add(hist);
						// ָ��ʱ����ƫ��
						pointerTime += intervalTime;
						// ֵ�����ݼ���
						tempNum++;

					}

				}

			}
			// ������һ��ֵ��ʱ�䲻��ָ��ʱ�䣬������ӵ�ֵ������count��
			if (pointerTime != lastTime && tempNum != count) {
				// ȡ���һ����ʵ��ֵ
				HisValue hisValue = history.get(history.size() - 1);
				// �����ǰ��Ҫ��������ֵ
				long num = (endTime - lastTime) / intervalTime;
				// ѭ����ֵ
				for (int j = 0; j < num; j++) {
					HisValue h = new HisValue();
					h.setId(hisValue.getId());
					h.setPointName(hisValue.getPointName());
					h.setTime(pointerTime);
					h.setType(hisValue.getType());
					h.setValue(hisValue.getState());
					list.add(h);
					// ָ��ʱ����ƫ��
					pointerTime += intervalTime;
					// ����ֵ�ĸ���
					tempNum++;
				}

			}

		}
		return list;
	}

	/**
	 * ����ָ��������ȡ�ñ�ǩ����һ��ʱ���ڵ�ͳ��ֵ(���ֵ����Сֵ��ƽ��ֵ)
	 * 
	 * @param pointName
	 *            ��ǩ��ȫ����
	 * @param startTime
	 *            ��ʼʱ��
	 * @param endTime
	 *            ����ʱ��
	 * @param intervalTime
	 *            ���ʱ�䣨��λ�����룩
	 * @return ͳ��ֵ�ļ���
	 * @throws KuduException
	 */
	@Override
	public List<StatisValue> getStatisValue(String pointName, long startTime, long endTime, long intervalTime)
			throws IllegalArgumentException, KuduException {
		// ���Ҫ�ֵĶ���
		long count = ((endTime - startTime) / intervalTime);
		// �����ʱ��ε���������
		List<HisValue> history = getHistory(pointName, startTime, endTime);
		// �����������ݵļ���
		List<StatisValue> statis = new ArrayList<StatisValue>();

		// ����ȡ��ֵ��Ϊ�գ��������ݼ���
		if (history != null && history.size() > 0) {
			// ȡ����һ����ʵֵ��ʱ��
			long firstTiem = history.get(0).getTime();
			// ���һ����ʵֵ��ʱ��
			// long lastTime = history.get(history.size() - 1).getTime();
			// ָ��ʱ��
			long pointerTime = startTime;
			// �����ǰ ��һֵ��ʱ����ڵڼ���ʱ�����
			long a = (firstTiem - startTime) / intervalTime;
			// ���a������ 0 ��˵����һ����ʵֵ���ڵ�һ��ʱ�����������Ҫ����ʱ��εĸ����������ݡ�
			if (a != 0) {
				// ��ȡָ����ʼʱ�����һ��ֵ
				HisValue uv = getUpperValue(pointName, startTime);
				double value = uv.getValue();
				// ���ͷ����ȱ����,���ݿ�ȱ��ʱ��θ���
				for (int i = 0; i < a; i++) {
					StatisValue s = new StatisValue();
					HisValue maxValue = new HisValue();
					HisValue minValue = new HisValue();
					HisValue avgValue = new HisValue();
					maxValue.setValue(value);
					maxValue.setTime(pointerTime);
					minValue.setValue(value);
					minValue.setTime(pointerTime);
					avgValue.setValue(value);
					avgValue.setTime(pointerTime);
					s.setAvgValue(avgValue);
					s.setMaxValue(maxValue);
					s.setMinValue(minValue);
					statis.add(s);
					// ʱ����ƫ��
					pointerTime += intervalTime;
				}
			}
			// �õ�����ʵֵ�Ŀ�ʼʱ��Σ���ָ�뿪ʼʱ�䣩
			long tempTime = startTime + intervalTime * a;
			// �����Чֵ�ĸ���
			int valueNum = (int) (count - a);
			List<List<HisValue>> countList = new ArrayList<>();
			// ��ԭ�����ݷֶ�
			List<HisValue> innerList = null;
			// ����ʵ���ݰ��ֶ�ʱ����֯�������
			for (int i = 0; i < history.size(); i++) {
				// �����ǰֵ����ʵʱ��
				long cruuTime = history.get(i).getTime();
				// ����ǵ�һ�λ� ��ʵֵ����ָ��ʱ��
				if (i == 0 || cruuTime > tempTime) {
					// ��ָ��ʱ������һ��ƫ������intervalTime
					tempTime += intervalTime;
					// ����һ���µļ��ϣ����ڱ���ö��ڵ�����
					innerList = new ArrayList<>();
					// �����ö��ڵ����ݼ����浽�ⲿ������
					countList.add(innerList);
					valueNum++;
				}
				// �����ǰֵ��ʱ��
				if (cruuTime <= tempTime && innerList != null) {
					innerList.add(history.get(i));
				}

			}

			// ���ֶ����ݽ��м���
			for (int i = 0; i < countList.size(); i++) {
				// ��ȡ���鼯���е�Ԫ��
				List<HisValue> list = countList.get(i);
				// �����ǰ�ļ�����û������
				if (list.size() == 0) {
					// �ͻ�ȡ��һ���ֶμ����е����һԪ�ص�ֵ
					int n = i;
					while (true) {
						n--;
						List<HisValue> v = countList.get(n);
						if (v.size() != 0) {
							HisValue hisValue = v.get(v.size() - 1);
							list.add(hisValue);
							break;
						}
					}

				}
				if (i != 0) {
					pointerTime += intervalTime;
				}
				StatisValue sv = CalculationEntity.statisValue(list, pointerTime);
				statis.add(sv);
			}
			// ����ۼӵĸ��������ڼ��������˵��β��û������
			if (valueNum != count) {
				// ����β�����ĸ���
				long num = count - valueNum;
				// ��ȡ��ʵ���ݵ����һ��ֵ
				HisValue hisValue = history.get(history.size() - 1);
				for (int i = 0; i < num; i++) {
					StatisValue s = new StatisValue();
					HisValue maxValue = new HisValue();
					HisValue minValue = new HisValue();
					HisValue avgValue = new HisValue();
					double value = hisValue.getValue();
					maxValue.setValue(value);
					maxValue.setTime(pointerTime);
					minValue.setValue(value);
					minValue.setTime(pointerTime);
					avgValue.setValue(value);
					avgValue.setTime(pointerTime);
					s.setAvgValue(avgValue);
					s.setMaxValue(maxValue);
					s.setMinValue(minValue);
					statis.add(s);
					pointerTime += intervalTime;
				}

			}
		}
		return statis;
	}

	@Override
	public HisValue getUpperValue(String pointName, long time) throws KuduException {

		HisValue v = null;
		time -= 1000;
		for (long i = time; i > 0; i -= 1000) {
			v = getHistoryInTime(pointName, i);
			if (v != null) {
				break;
			}
		}

		return v;
	}

	/**
	 * ������ȡ��ǩ��Ķ�������
	 * 
	 * @param pointName
	 *            ��ǩ�����Ƶļ���
	 * @param time
	 *            ָ����ʱ��
	 * @return �������ݵļ���
	 */
	@Override
	public List<HisValue> getSectionHistory(String[] pointName, long time) {
		List<HisValue> lists = new ArrayList<HisValue>();
		for (String disPointName : pointName) {
			PointMap pm = arrangementPoint(disPointName);

			String tableName = pm.getTableName();
			String columnName = pm.getColumnName();
			String id = pm.getId();

			KuduTable table = null;
			try {
				table = client.openTable(tableName);
				if ((table.getName() == null) || table.getName() == "") {
					logger.info("��������ȷ����ʧ��...");
				}
			} catch (KuduException e1) {

				e1.printStackTrace();
			}

			List<String> projectColumns = new ArrayList<String>(3);
			projectColumns.add("id");
			projectColumns.add("time");
			projectColumns.add(columnName);
			String searchtime = DateUtils.longToString(time, "yyyy-MM-dd HH:mm:ss");
			KuduPredicate predicate1 = KuduPredicate.newComparisonPredicate(table.getSchema().getColumn("time"),
					ComparisonOp.EQUAL, searchtime);
			KuduPredicate predicate2 = KuduPredicate.newComparisonPredicate(table.getSchema().getColumn("id"),
					ComparisonOp.EQUAL, id);
			KuduScanner scb = client.newScannerBuilder(table).addPredicate(predicate1).addPredicate(predicate2)
					.setProjectedColumnNames(projectColumns).build();
			HisValue resultVal = null;
			while (scb.hasMoreRows()) {
				RowResultIterator iterator = null;
				try {
					iterator = scb.nextRows();
					while (iterator.hasNext()) {
						resultVal = new HisValue();
						RowResult result = iterator.next();
						String name = table.getSchema().getColumn(columnName).getType().getName().trim();
						String ids = result.getString("id");
						resultVal.setPointName(disPointName);
						resultVal.parseType(name, columnName, result, resultVal);
						resultVal.setTime(DateUtils.stringToDate(result.getString("time")).getTime());
						resultVal.setId(ids);
					}

				} catch (KuduException e) {
					e.printStackTrace();
				}
			}
			if (resultVal != null) {
				lists.add(resultVal);
			}
			try {
				scb.close();
			} catch (KuduException e) {
				e.printStackTrace();
			}
		}

		return lists;
	}

	private PointMap arrangementPoint(String tagName) {

		tagName = tagName.toUpperCase();
		PointMap p = new PointMap();
		String columnName = "";
		String tableName = "";
		String id = "";
		if (tagName.contains("_FJ_")) {
			// a.�Ƿ����
			columnName = tagName.substring(tagName.lastIndexOf("_") + 1, tagName.length());
			id = tagName.substring(0, tagName.lastIndexOf("_"));
			tableName = mappingMap.get(columnName);
		} else {
			// b.���Ƿ����
			columnName = tagName;
			id = tagName.substring(0, tagName.lastIndexOf("_"));
			tableName = mappingMap.get(columnName);
		}
		p.setColumnName(columnName);
		p.setTableName(tableName);
		p.setId(id);

		return p;

	}

	/**
	 * ������ʷֵ
	 * 
	 * @param value
	 *            ����������ʵ�� ��ʷֵʵ��
	 * @param tableName
	 *            �����ı�����
	 * 
	 * @return �Ƿ����ɹ�
	 * @throws KuduException
	 * @throws Exception
	 */
	@Override
	public boolean intsertPointData(ValueModel value, String tableName) throws Exception {
		boolean flag = false;
		String pointName = value.getPointName();
		String id = value.getId();
		long time = value.getTime();
		Type type = value.getType();
		String columnName = pointName;
		if (pointName.contains("_FJ_") || tableName == null) {
			PointMap point = arrangementPoint(pointName);
			tableName = point.getTableName();
			id = point.getId();
			columnName = point.getColumnName();
		}
		KuduSession session = client.newSession();
		KuduTable table = client.openTable(tableName);
		Object valu = value.getValue();
		if (id != null && time != 0) {
			String searchtime = DateUtils.longToString(time, "yyyy-MM-dd HH:mm:ss");
			Insert insert = table.newInsert();
			PartialRow row = insert.getRow();
			row.addString(0, id);
			row.addString(1, searchtime);
			addValue(row, type, valu, columnName);
			OperationResponse apply = session.apply(insert);
			if (!apply.hasRowError()) {
				flag = true;
			}
		}
		session.close();
		return flag;
	}

	private void addValue(PartialRow row, Type type, Object valus, String columnName) throws Exception {
		if (valus == null) {
			throw new Exception("ֵΪ�գ�");
		}
		if (type == Type.BOOL) {
			int state = (int) valus;
			row.addBoolean(columnName, state == 1 ? true : false);
		} else if (type == Type.DOUBLE) {
			double valu = (double) valus;
			row.addDouble(columnName, valu);
		} else if (type == Type.FLOAT) {
			float valu = (float) valus;
			row.addFloat(columnName, valu);
		} else if (type == Type.INT16) {
			short state = (short) valus;
			row.addShort(columnName, state);
		} else if (type == Type.INT32) {
			int state = (int) valus;
			row.addInt(columnName, state);
		} else if (type == Type.INT64) {
			long state = (long) valus;
			row.addLong(columnName, state);
		} else if (type == Type.STRING) {
			String str = (String) valus;
			row.addString(columnName, str);
		}
	}

	/**
	 * �޸���ʷֵ
	 * 
	 * @param value
	 *            ����������ʵ��
	 * @param tableName
	 *            �����ı�����
	 * @return �Ƿ��޸ĳɹ�
	 * @throws KuduException
	 * @throws Exception
	 */
	@Override
	public boolean updatePointData(ValueModel value, String tableName) throws Exception {
		boolean flag = false;
		String pointName = value.getPointName();
		String id = value.getId();
		long time = value.getTime();
		Type type = value.getType();
		String columnName = pointName;
		if (pointName.contains("_FJ_") || tableName == null) {
			PointMap point = arrangementPoint(pointName);
			tableName = point.getTableName();
			id = point.getId();
			columnName = point.getColumnName();
		}

		KuduSession session = client.newSession();
		KuduTable table = client.openTable(tableName);
		Object valu = value.getValue();

		if (id != null && time != 0) {
			String searchtime = DateUtils.longToString(time, "yyyy-MM-dd HH:mm:ss");
			Update update = table.newUpdate();
			PartialRow row = update.getRow();
			row.addString(0, id);
			row.addString(1, searchtime);
			addValue(row, type, valu, columnName);
			OperationResponse apply = session.apply(update);
			if (!apply.hasRowError()) {
				flag = true;
			}
		}
		session.close();
		return flag;
	}

	@Override
	public boolean upSertPointData(ValueModel value, String tableName) throws Exception {
		boolean flag = false;
		String pointName = value.getPointName();
		String id = value.getId();
		long time = value.getTime();
		Type type = value.getType();
		String columnName = pointName;
		if (pointName.contains("_FJ_") || tableName == null) {
			PointMap point = arrangementPoint(pointName);
			tableName = point.getTableName();
			id = point.getId();
			columnName = point.getColumnName();
		}

		KuduSession session = client.newSession();
		KuduTable table = client.openTable(tableName);
		Object valu = value.getValue();
		if (id != null && time != 0) {
			String searchtime = DateUtils.longToString(time, "yyyy-MM-dd HH:mm:ss");
			Upsert update = table.newUpsert();
			PartialRow row = update.getRow();
			row.addString(0, id);
			row.addString(1, searchtime);
			addValue(row, type, valu, columnName);
			OperationResponse apply = session.apply(update);
			if (!apply.hasRowError()) {
				flag = true;
			}

		}
		session.close();
		return flag;
	}

	@Override
	public List<ValueData> getSnapshots(int[] ids) throws UnknownHostException, IOException, Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<ValueData> getSnapshots(String[] pointName) throws UnknownHostException, IOException, Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
