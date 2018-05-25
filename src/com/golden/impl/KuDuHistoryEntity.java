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
				logger.info("获取映射表数据失败.....");
				e.printStackTrace();
			}
		}
	}

	private void initMappingMap() throws KuduException {
		logger.debug("正在加载映射表信息....");
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
			logger.error("加载映射表信息失败！");
		}
		scaner.close();
	}

	/**
	 * 根据标签点名称获取所有历史数据
	 * 
	 * @param pointName
	 *            标签点全名称
	 * @return 历史数据的集合
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
			logger.info("表名不正确，打开失败...");
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
		// 获取该列的类型
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
	 * 根据标签点名称获取一段时间内的历史值
	 * 
	 * @param pointName
	 *            标签点全名称
	 * @param startTime
	 *            开始时间
	 * @param endTime
	 *            结束时间
	 * @return 所查数据的集合
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
			logger.info("表名不正确，打开失败...");
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
		// 获取该列的类型
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
	 * 根据标签点获取指定时间的历史值
	 * 
	 * @param pointName
	 *            标签点全名称
	 * @param time
	 *            所指定的时间
	 * @return 历史数据
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
			logger.info("表名不正确，打开失败...");
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
		// 获取该列的类型
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
	 * 根据指定条件获取指定标签点的等间隔值
	 * 
	 * @param pointName
	 *            标签点全名称
	 * @param startTime
	 *            开始时间
	 * @param endTime
	 *            结束时间
	 * @param intervalTime
	 *            间隔时间（单位：毫秒）
	 * @return 符合条件的历史数据的集合
	 * @throws KuduException
	 */
	@Override
	public List<HisValue> getIntervalHistory(String pointName, long startTime, long endTime, long intervalTime)
			throws KuduException {
		long vasTime = endTime - startTime;
		// 计算出实际获取的等间隔值的个数
		long count = vasTime / intervalTime;
		// 获取该时间段的所有的数据
		//long s = System.currentTimeMillis();
		List<HisValue> history = getHistory(pointName, startTime, endTime);
		//System.out.println(System.currentTimeMillis() - s);
		List<HisValue> list = new ArrayList<HisValue>();
		// 指针时间
		long pointerTime = startTime;
		long tempNum = 0;
		if (history != null) {
			// 获取真实值集合的第一个值的时间
			long fristTime = history.get(0).getTime();
			long lastTime = history.get(history.size() - 1).getTime();
			// 当前第一个值的时间不等于开始时间
			if (fristTime > startTime && tempNum == 0) {
				// 取上一个值。
				HisValue upperValue = getUpperValue(pointName, pointerTime);
				// 计算出前面要补几个个值
				long num = (fristTime - startTime) / intervalTime;
				// 循环补值
				for (int j = 0; j <= num; j++) {
					HisValue h = new HisValue();
					h.setId(upperValue.getId());
					h.setPointName(upperValue.getPointName());
					h.setTime(pointerTime);
					h.setType(upperValue.getType());
					h.setValue(upperValue.getState());
					list.add(h);
					// 指针时间作偏移
					pointerTime += intervalTime;
					// 计算值的个数
					tempNum++;
				}
			}
			for (int i = 0; i < history.size(); i++) {
				// 获取当前值的时间
				long realTime = history.get(i).getTime();
				// 判断当前值不是开始时间的值，这种情况只有当前值的时间小于开始时间，所以要补值。
				if (realTime != pointerTime) {

					// 当前值的时间大于指针时间，就要取上一个值的数据
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
							// 指针时间作偏移
							pointerTime += intervalTime;
							// 值个数据计算
							tempNum++;
							i--;

						}

					}

				} else if (realTime == pointerTime) {
					// 当前时间是指偏移时间，说明在指定的时间点上，正常处理
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
						// 指针时间作偏移
						pointerTime += intervalTime;
						// 值个数据计算
						tempNum++;

					}

				}

			}
			// 如果最后一个值的时间不是指针时间，并且添加的值不等于count个
			if (pointerTime != lastTime && tempNum != count) {
				// 取最后一个真实的值
				HisValue hisValue = history.get(history.size() - 1);
				// 计算出前面要补几个个值
				long num = (endTime - lastTime) / intervalTime;
				// 循环补值
				for (int j = 0; j < num; j++) {
					HisValue h = new HisValue();
					h.setId(hisValue.getId());
					h.setPointName(hisValue.getPointName());
					h.setTime(pointerTime);
					h.setType(hisValue.getType());
					h.setValue(hisValue.getState());
					list.add(h);
					// 指针时间作偏移
					pointerTime += intervalTime;
					// 计算值的个数
					tempNum++;
				}

			}

		}
		return list;
	}

	/**
	 * 根据指定条件获取该标签点在一段时间内的统计值(最大值，最小值，平均值)
	 * 
	 * @param pointName
	 *            标签点全名称
	 * @param startTime
	 *            开始时间
	 * @param endTime
	 *            结束时间
	 * @param intervalTime
	 *            间隔时间（单位：毫秒）
	 * @return 统计值的集合
	 * @throws KuduException
	 */
	@Override
	public List<StatisValue> getStatisValue(String pointName, long startTime, long endTime, long intervalTime)
			throws IllegalArgumentException, KuduException {
		// 求出要分的段数
		long count = ((endTime - startTime) / intervalTime);
		// 查出该时间段的所有数据
		List<HisValue> history = getHistory(pointName, startTime, endTime);
		// 创建保存数据的集合
		List<StatisValue> statis = new ArrayList<StatisValue>();

		// 当获取的值不为空，进行数据计算
		if (history != null && history.size() > 0) {
			// 取出第一个真实值的时间
			long firstTiem = history.get(0).getTime();
			// 最后一个真实值的时间
			// long lastTime = history.get(history.size() - 1).getTime();
			// 指针时间
			long pointerTime = startTime;
			// 求出当前 第一值的时间戳在第几个时间段内
			long a = (firstTiem - startTime) / intervalTime;
			// 如果a不等于 0 ，说明第一个真实值不在第一个时间段里，这里就需要根据时间段的个数来补数据。
			if (a != 0) {
				// 获取指定开始时间的上一个值
				HisValue uv = getUpperValue(pointName, startTime);
				double value = uv.getValue();
				// 添加头部空缺数据,根据空缺的时间段个数
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
					// 时间作偏移
					pointerTime += intervalTime;
				}
			}
			// 得到有真实值的开始时间段（即指针开始时间）
			long tempTime = startTime + intervalTime * a;
			// 算出有效值的个数
			int valueNum = (int) (count - a);
			List<List<HisValue>> countList = new ArrayList<>();
			// 将原有数据分段
			List<HisValue> innerList = null;
			// 将真实数据按分段时间组织拆分数据
			for (int i = 0; i < history.size(); i++) {
				// 求出当前值的真实时间
				long cruuTime = history.get(i).getTime();
				// 如果是第一次或 真实值大于指针时间
				if (i == 0 || cruuTime > tempTime) {
					// 将指针时间增加一个偏移量，intervalTime
					tempTime += intervalTime;
					// 创建一个新的集合，用于保存该段内的数据
					innerList = new ArrayList<>();
					// 并将该段内的数据集保存到外部集合中
					countList.add(innerList);
					valueNum++;
				}
				// 如果当前值的时间
				if (cruuTime <= tempTime && innerList != null) {
					innerList.add(history.get(i));
				}

			}

			// 将分段数据进行计算
			for (int i = 0; i < countList.size(); i++) {
				// 获取分组集合中的元素
				List<HisValue> list = countList.get(i);
				// 如果当前的集合中没有数据
				if (list.size() == 0) {
					// 就获取上一个分段集合中的最后一元素的值
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
			// 如果累加的个数不等于计算个数，说明尾部没有数据
			if (valueNum != count) {
				// 计算尾部相差的个数
				long num = count - valueNum;
				// 获取真实数据的最后一个值
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
	 * 批量获取标签点的断面数据
	 * 
	 * @param pointName
	 *            标签点名称的集合
	 * @param time
	 *            指定的时间
	 * @return 断面数据的集合
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
					logger.info("表名不正确，打开失败...");
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
			// a.是风机点
			columnName = tagName.substring(tagName.lastIndexOf("_") + 1, tagName.length());
			id = tagName.substring(0, tagName.lastIndexOf("_"));
			tableName = mappingMap.get(columnName);
		} else {
			// b.不是风机点
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
	 * 插入历史值
	 * 
	 * @param value
	 *            需插入的数据实体 历史值实体
	 * @param tableName
	 *            操作的表名称
	 * 
	 * @return 是否插入成功
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
			throw new Exception("值为空！");
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
	 * 修改历史值
	 * 
	 * @param value
	 *            需插入的数据实体
	 * @param tableName
	 *            操作的表名称
	 * @return 是否修改成功
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
