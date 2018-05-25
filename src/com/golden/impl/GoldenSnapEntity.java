package com.golden.impl;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.kudu.client.KuduException;

import com.golden.interfac.GoldenDataSource;
import com.golden.model.HisValue;
import com.golden.model.StatisValue;
import com.golden.model.ValueModel;
import com.rtdb.api.model.ValueData;
import com.rtdb.model.MinPoint;
import com.rtdb.service.impl.BaseImpl;
import com.rtdb.service.impl.ServerImpl;
import com.rtdb.service.impl.SnapshotImpl;

/**
 * 
 * ��ȡ�������ݵ�ʵ����
 *
 */
public class GoldenSnapEntity implements GoldenDataSource {

	private ServerImpl server;

	/**
	 * 
	 * @param ip
	 * @param port
	 * @param userName
	 * @param password
	 * @throws UnknownHostException
	 * @throws IOException
	 * @throws Exception
	 */
	public GoldenSnapEntity(String ip, int port, String userName, String password)
			throws UnknownHostException, IOException, Exception {
		this.server = new ServerImpl(ip, port, userName, password);

	}

	/**
	 * ���ݱ�ǩ��ID������ȡ�������ݡ�
	 * 
	 * @param ids
	 *            �������ݿ��ж�Ӧ�ı�ǩ��ID����
	 * @return ���ݼ���
	 * @throws Exception
	 * @throws IOException
	 * @throws UnknownHostException
	 */
	@Override
	public List<ValueData> getSnapshots(int[] ids) throws UnknownHostException, IOException, Exception {
		SnapshotImpl snap = new SnapshotImpl(server);
		List<ValueData> reValue = snap.getSnapshots(ids);
		return reValue;
	}

	/**
	 * ���ݱ�ǩ��ID������ȡ�������ݡ�
	 * 
	 * @param pointName
	 *            ��ǩ�����Ƶļ���
	 * @return �������ݵļ���
	 * @throws Exception
	 * @throws IOException
	 * @throws UnknownHostException
	 */
	@Override
	public List<ValueData> getSnapshots(String[] pointName) throws UnknownHostException, IOException, Exception {
		BaseImpl base = new BaseImpl(server);
		List<MinPoint> list = base.findPoints(pointName);
		List<ValueData> reValue = null;
		if (list != null) {

			int[] ids = new int[list.size()];
			for (int i = 0; i < list.size(); i++) {
				ids[i] = list.get(i).getId();

			}
			reValue = getSnapshots(ids);
		}
		return reValue;
	}

	@Override
	public List<HisValue> getHistory(String pointName) throws UnknownHostException, IOException, Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<HisValue> getHistory(String pointName, long startTime, long endTime) throws KuduException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public HisValue getHistoryInTime(String pointName, long time) throws KuduException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<HisValue> getIntervalHistory(String pointName, long startTime, long endTime, long intervalTime)
			throws KuduException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<StatisValue> getStatisValue(String pointName, long startTime, long endTime, long intervalTime)
			throws IllegalArgumentException, KuduException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean intsertPointData(ValueModel value, String tableName) throws KuduException, Exception {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean updatePointData(ValueModel value, String tableName) throws KuduException, Exception {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean upSertPointData(ValueModel value, String tableName) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public HisValue getUpperValue(String pointName, long time) throws KuduException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<HisValue> getSectionHistory(String[] pointName, long time) {
		// TODO Auto-generated method stub
		return null;
	}

}
