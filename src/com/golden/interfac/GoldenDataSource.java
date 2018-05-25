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
 * 数据源服务类
 *
 */
public interface GoldenDataSource {
	/**
	 * 根据标签点名称获取所有历史数据
	 * 
	 * @param pointName
	 *            标签点全名称
	 * @return 历史数据的集合
	 * @throws Exception
	 * @throws IOException
	 * @throws UnknownHostException
	 */
	public abstract List<HisValue> getHistory(String pointName) throws UnknownHostException, IOException, Exception;

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
	public abstract List<HisValue> getHistory(String pointName, long startTime, long endTime) throws KuduException;

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
	public abstract HisValue getHistoryInTime(String pointName, long time) throws KuduException;

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
	public abstract List<HisValue> getIntervalHistory(String pointName, long startTime, long endTime, long intervalTime)
			throws KuduException;

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
	 * @throws IllegalArgumentException
	 */
	public abstract List<StatisValue> getStatisValue(String pointName, long startTime, long endTime, long intervalTime)
			throws IllegalArgumentException, KuduException;

	/**
	 * 插入历史值
	 * 
	 * @param value
	 *            需插入的数据实体 历史值实体
	 * @param tableName
	 *            表名称，该参数据可以给值也可以为null;
	 * @return 是否插入成功
	 * @throws KuduException
	 * @throws Exception
	 */
	public abstract boolean intsertPointData(ValueModel value, String tableName) throws KuduException, Exception;

	/**
	 * 修改历史值
	 * 
	 * @param value
	 *            需插入的数据实体
	 * @param tableName
	 *            表名称，该参数据可以给值也可以为null;
	 * @return 是否修改成功
	 * @throws KuduException
	 * @throws Exception
	 */

	public abstract boolean updatePointData(ValueModel value, String tableName) throws KuduException, Exception;

	/**
	 * 插入更新
	 * <p>
	 * 该API会先判断该记录是否存在，如果存在会进行更新操作，如果不存在会插入一条新的数据
	 * </p>
	 * 
	 * @param value
	 *            需插入的数据实体
	 * @param tableName
	 *            表名称，该参数据可以给值也可以为null;
	 * 
	 * @return 是否操作成功
	 * @throws Exception
	 */
	public boolean upSertPointData(ValueModel value, String tableName) throws Exception;

	/**
	 * 根据标签点获取指定时间 的上一条数据
	 * 
	 * @param pointName
	 *            标签点名称
	 * @param time
	 *            指定的时间
	 * 
	 * @return 最接近该指定时间的上一条数据
	 * @throws KuduException
	 */
	public HisValue getUpperValue(String pointName, long time) throws KuduException;
	/**
	 * 批量获取标签点的断面数据
	 * 
	 * @param pointName
	 *            标签点名称的集合
	 * @param time
	 *            指定的时间（单位：毫秒）
	 * @return 返回指定时间历史数据的集合
	 */
	public List<HisValue> getSectionHistory(String[] pointName, long time);
	// -----------------------------------实时数据类API-----------------

	/**
	 * 根据标签点ID批量获取快照数据。
	 * 
	 * @param ids
	 *            标签点 id的集合
	 * @return 快照数据的集合
	 * @throws Exception
	 * @throws IOException
	 * @throws UnknownHostException
	 */
	public List<ValueData> getSnapshots(int[] ids) throws UnknownHostException, IOException, Exception;

	/**
	 * 根据标签点ID批量获取快照数据。
	 * 
	 * @param pointName
	 *            标签点名称的集合
	 * @return 快照数据的集合
	 * @throws Exception
	 * @throws IOException
	 * @throws UnknownHostException
	 */
	public List<ValueData> getSnapshots(String[] pointName) throws UnknownHostException, IOException, Exception;

	

}
