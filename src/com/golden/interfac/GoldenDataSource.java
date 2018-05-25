package com.golden.interfac;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.kudu.client.KuduException;

import com.golden.model.HisValue;
import com.golden.model.StatisValue;
import com.golden.model.ValueModel;
import com.rtdb.api.model.ValueData;

/**
 * ����Դ������
 *
 */
public interface GoldenDataSource {
	/**
	 * ���ݱ�ǩ�����ƻ�ȡ������ʷ����
	 * 
	 * @param pointName
	 *            ��ǩ��ȫ����
	 * @return ��ʷ���ݵļ���
	 * @throws Exception
	 * @throws IOException
	 * @throws UnknownHostException
	 */
	public abstract List<HisValue> getHistory(String pointName) throws UnknownHostException, IOException, Exception;

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
	public abstract List<HisValue> getHistory(String pointName, long startTime, long endTime) throws KuduException;

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
	public abstract HisValue getHistoryInTime(String pointName, long time) throws KuduException;

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
	public abstract List<HisValue> getIntervalHistory(String pointName, long startTime, long endTime, long intervalTime)
			throws KuduException;

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
	 * @throws IllegalArgumentException
	 */
	public abstract List<StatisValue> getStatisValue(String pointName, long startTime, long endTime, long intervalTime)
			throws IllegalArgumentException, KuduException;

	/**
	 * ������ʷֵ
	 * 
	 * @param value
	 *            ����������ʵ�� ��ʷֵʵ��
	 * @param tableName
	 *            �����ƣ��ò����ݿ��Ը�ֵҲ����Ϊnull;
	 * @return �Ƿ����ɹ�
	 * @throws KuduException
	 * @throws Exception
	 */
	public abstract boolean intsertPointData(ValueModel value, String tableName) throws KuduException, Exception;

	/**
	 * �޸���ʷֵ
	 * 
	 * @param value
	 *            ����������ʵ��
	 * @param tableName
	 *            �����ƣ��ò����ݿ��Ը�ֵҲ����Ϊnull;
	 * @return �Ƿ��޸ĳɹ�
	 * @throws KuduException
	 * @throws Exception
	 */

	public abstract boolean updatePointData(ValueModel value, String tableName) throws KuduException, Exception;

	/**
	 * �������
	 * <p>
	 * ��API�����жϸü�¼�Ƿ���ڣ�������ڻ���и��²�������������ڻ����һ���µ�����
	 * </p>
	 * 
	 * @param value
	 *            ����������ʵ��
	 * @param tableName
	 *            �����ƣ��ò����ݿ��Ը�ֵҲ����Ϊnull;
	 * 
	 * @return �Ƿ�����ɹ�
	 * @throws Exception
	 */
	public boolean upSertPointData(ValueModel value, String tableName) throws Exception;

	/**
	 * ���ݱ�ǩ���ȡָ��ʱ�� ����һ������
	 * 
	 * @param pointName
	 *            ��ǩ������
	 * @param time
	 *            ָ����ʱ��
	 * 
	 * @return ��ӽ���ָ��ʱ�����һ������
	 * @throws KuduException
	 */
	public HisValue getUpperValue(String pointName, long time) throws KuduException;
	/**
	 * ������ȡ��ǩ��Ķ�������
	 * 
	 * @param pointName
	 *            ��ǩ�����Ƶļ���
	 * @param time
	 *            ָ����ʱ�䣨��λ�����룩
	 * @return ����ָ��ʱ����ʷ���ݵļ���
	 */
	public List<HisValue> getSectionHistory(String[] pointName, long time);
	// -----------------------------------ʵʱ������API-----------------

	/**
	 * ���ݱ�ǩ��ID������ȡ�������ݡ�
	 * 
	 * @param ids
	 *            ��ǩ�� id�ļ���
	 * @return �������ݵļ���
	 * @throws Exception
	 * @throws IOException
	 * @throws UnknownHostException
	 */
	public List<ValueData> getSnapshots(int[] ids) throws UnknownHostException, IOException, Exception;

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
	public List<ValueData> getSnapshots(String[] pointName) throws UnknownHostException, IOException, Exception;

	

}
