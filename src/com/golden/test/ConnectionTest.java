package com.golden.test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;

import org.apache.kudu.Type;
import org.apache.kudu.client.KuduException;

import com.golden.impl.GoldenSnapEntity;
import com.golden.impl.KuDuHistoryEntity;
import com.golden.interfac.GoldenDataSource;
import com.golden.model.HisValue;
import com.golden.model.StatisValue;
import com.golden.model.ValueModel;
import com.golden.util.DateUtils;
import com.rtdb.api.model.ValueData;

public class ConnectionTest {
	public static void main(String[] args) throws UnknownHostException, IOException, Exception {
		// 获取单个标签点的历史数据
		// getHistory1();// 测试通过

		// 获取单个标签点一段时间内的历史数据
		// getHistory2();//测试通过

		// 获取标签点指定时间的历史值
		// getHistory3();//测试通过

		// 获取标签点指定时间内的等间隔值
		//System.setOut(new PrintStream(new File("f:\\p.txt")));
		getHistory4();

		// 获取标签点指定时间内的统计值（最大值，最小值，平均值）
		// getHistory5();//测试通过

		// 向表中写入一条数据
		// intsertPointData();//测试通过

		// 更新一条数据
		// updatePointData();//测试通过

		// 获取指定时间的上一条数据
		// getUpperValue();//测试通过

		// 更新或插入一条数据
		// upSertPointData();//测试通过

		// 批量获取标签点的快照数据
		// getSnapshots();//测试通过

	}

	// 获取单个标签点的历史数据
	public static void getHistory1() throws UnknownHostException, IOException, Exception {
		String kuduMaster = "172.168.1.38:7051,172.168.1.39:7051";
		String mappingTabel = "gdnxfd.NSS_point";
		GoldenDataSource gs = new KuDuHistoryEntity(kuduMaster, mappingTabel);
		String pointName = "NX_GD_NSSF_DQ_P1_L1_001_AI0884";
		// String pointName = "NX_GD_NSSF_DD_P1_L1_001_FXWG008";
		List<HisValue> history = gs.getHistory(pointName);
		for (int i = 0; i < history.size(); i++) {
			HisValue hisValue = history.get(i);
			System.out.println(hisValue.getId() + "\t" + DateUtils.dateToString(new Date(hisValue.getTime())) + "\t"
					+ hisValue.getValue() + "\t" + hisValue.getPointName());
		}
	}

	// 获取单个标签点一段时间内的历史数据
	public static void getHistory2() throws Exception {
		String kuduMaster = "172.168.1.38:7051,172.168.1.39:7051";
		String mappingTabel = "gdnxfd.NSS_point";
		GoldenDataSource gs = new KuDuHistoryEntity(kuduMaster, mappingTabel);
		String pointName = "NX_GD_NSSF_DQ_P1_L1_001_AI0884";
		long st = DateUtils.stringToDate("2018-03-15 12:05:27").getTime();
		long et = DateUtils.stringToDate("2018-03-15 12:05:39").getTime();
		List<HisValue> history = gs.getHistory(pointName, st, et);
		for (int i = 0; i < history.size(); i++) {
			HisValue hisValue = history.get(i);
			System.out.println(hisValue.getId() + "\t" + DateUtils.dateToString(new Date(hisValue.getTime())) + "\t"
					+ hisValue.getValue());
		}
	}

	// 获取标签点指定时间的历史值
	public static void getHistory3() throws KuduException {
		String kuduMaster = "172.168.1.38:7051,172.168.1.39:7051";
		String mappingTabel = "gdnxfd.NSS_point";
		GoldenDataSource gs = new KuDuHistoryEntity(kuduMaster, mappingTabel);
		String pointName = "NX_GD_NSSF_DQ_P1_L1_001_AI0884";
		// String pointName = "NX_GD_NSSF_DD_P1_L1_001_FXWG008";
		long time = DateUtils.stringToDate("2018-03-15 12:45:30").getTime();
		HisValue hisValue = gs.getHistoryInTime(pointName, time);
		System.out.println(hisValue.getPointName() + "\t" + DateUtils.dateToString(new Date(hisValue.getTime())) + "\t"
				+ hisValue.getValue());
	}

	// 获取标签点指定时间内的等间隔值
	public static void getHistory4() throws KuduException {
		String kuduMaster = "172.168.1.38:7051,172.168.1.39:7051";
		String mappingTabel = "gdnxfd.NSS_point";
		GoldenDataSource gs = new KuDuHistoryEntity(kuduMaster, mappingTabel);
		String pointName = "NX_GD_NSSF_DQ_P1_L1_001_AI0884";
		long st = DateUtils.stringToDate("2018-03-12 12:05:27").getTime();
		long et = DateUtils.stringToDate("2018-03-15 12:05:39").getTime();
		long s = System.currentTimeMillis();
		List<HisValue> history = gs.getIntervalHistory(pointName, st, et, 5000);
		long b = System.currentTimeMillis() - s;
		int count =0;
		for (int i = 0; i < history.size(); i++) {
			HisValue hisValue = history.get(i);
			System.out.println(hisValue.getId() + "\t" + DateUtils.dateToString(new Date(hisValue.getTime())) + "\t"
					+ hisValue.getValue());
			count++;
		}
		System.out.println(b+"\t"+count);
	}

	// 获取标签点指定时间内的统计值(最大值，最小值，平均值)
	public static void getHistory5() throws IllegalArgumentException, KuduException {
		String kuduMaster = "172.168.1.38:7051,172.168.1.39:7051";
		String mappingTabel = "gdnxfd.NSS_point";
		GoldenDataSource gs = new KuDuHistoryEntity(kuduMaster, mappingTabel);
		String pointName = "NX_GD_NSSF_DQ_P1_L1_001_AI0884";
		long st = DateUtils.stringToDate("2018-03-14 12:05:27").getTime();
		long et = DateUtils.stringToDate("2018-03-15 12:05:39").getTime();
		List<StatisValue> statisValue = gs.getStatisValue(pointName, st, et, 60000);
		int count = 0;
		for (StatisValue sa : statisValue) {
			double avg = sa.getAvgValue().getValue();
			double max = sa.getMaxValue().getValue();
			double min = sa.getMinValue().getValue();
			System.out.println("最大值：" + max + "\t 最小值：" + min + "\t 平均值 ：" + avg);
			count++;
		}
		System.out.println(count);
	}

	// 向表中写入一条数据
	public static void intsertPointData() throws Exception {
		String kuduMaster = "172.168.1.38:7051,172.168.1.39:7051";
		String mappingTabel = "gdnxfd.NSS_point";
		GoldenDataSource gs = new KuDuHistoryEntity(kuduMaster, mappingTabel);
		String pointName = "NX_GD_NSSF_DQ_P1_L1_001_AI0884";
		long time = DateUtils.stringToDate("2018-03-15 13:15:34").getTime();
		ValueModel value = new ValueModel();
		value.setPointName(pointName);
		value.setId("NX_GD_NSSF_DD_P1_L1_001");
		value.setTime(time);
		value.setType(Type.FLOAT);
		value.setValue(368.6f);
		boolean reslut = gs.intsertPointData(value, null);
		System.out.println(reslut);
	}

	// 更新一条数据
	public static void updatePointData() throws Exception {
		String kuduMaster = "172.168.1.38:7051,172.168.1.39:7051";
		String mappingTabel = "gdnxfd.NSS_point";
		GoldenDataSource gs = new KuDuHistoryEntity(kuduMaster, mappingTabel);
		String pointName = "NX_GD_NSSF_DQ_P1_L1_001_AI0884";
		long time = DateUtils.stringToDate("2018-03-15 13:15:34").getTime();
		ValueModel value = new ValueModel();
		value.setPointName(pointName);
		value.setId("NX_GD_NSSF_DD_P1_L1_001");
		value.setTime(time);
		value.setType(Type.FLOAT);
		value.setValue(777f);
		boolean reslut = gs.updatePointData(value, null);
		System.out.println(reslut);
	}

	// 获取指定时间的上一条数据
	public static void getUpperValue() throws Exception {
		String kuduMaster = "172.168.1.38:7051,172.168.1.39:7051";
		String mappingTabel = "gdnxfd.NSS_point";
		GoldenDataSource gs = new KuDuHistoryEntity(kuduMaster, mappingTabel);
		String pointName = "NX_GD_NSSF_DQ_P1_L1_001_AI0884";
		long time = DateUtils.stringToDate("2018-03-15 13:15:31").getTime();
		long s = System.currentTimeMillis();
		HisValue hisValue = gs.getUpperValue(pointName, time);
		System.out.println(hisValue.getId() + "\t" + DateUtils.dateToString(new Date(hisValue.getTime())) + "\t"
				+ hisValue.getValue());
		System.out.println(System.currentTimeMillis() - s);
	}

	// 更新或插入一条数据
	public static void upSertPointData() throws Exception {
		String kuduMaster = "172.168.1.38:7051,172.168.1.39:7051";
		String mappingTabel = "gdnxfd.NSS_point";
		GoldenDataSource gs = new KuDuHistoryEntity(kuduMaster, mappingTabel);
		String pointName = "NX_GD_NSSF_DQ_P1_L1_001_AI0884";
		long time = DateUtils.stringToDate("2018-03-15 12:47:25").getTime();
		ValueModel value = new ValueModel();
		value.setPointName(pointName);
		value.setId("NX_GD_NSSF_DD_P1_L1_001");
		value.setTime(time);
		value.setType(Type.FLOAT);
		value.setValue(333f);
		boolean reslut = gs.upSertPointData(value, null);
		System.out.println(reslut);
	}

	// 批量获取标签点的快照数据
	public static void getSnapshots() throws Exception {
		GoldenDataSource gs = new GoldenSnapEntity("172.168.1.3", 6327, "sa", "golden");

		// String[] str = {"NSSFJ.NX_GD_NSSF_DD_P1_L1_001_FXWG008"};
		String[] str = { "nssfj.nx_gd_nssf_fj_p1_l1_001_ai0007", "nssfj.nx_gd_nssf_fj_p1_l1_001_ai0008",
				"nssfj.nx_gd_nssf_fj_p1_l1_001_ai0009", "nssfj.nx_gd_nssf_fj_p1_l1_001_ai0010" };

		List<ValueData> snapdata = gs.getSnapshots(str);
		for (int i = 0; i < snapdata.size(); i++) {
			ValueData valueData = snapdata.get(i);
			System.out.println(valueData.getId() + "\t" + valueData.getValue() + "\t" + valueData.getState() + "\t"
					+ DateUtils.dateToString(valueData.getDate()));

		}

	}

}
