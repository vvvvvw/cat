package com.dianping.cat.analysis;

public class PeriodStrategy {
	//每个周期的时间长度，默认为1个小时
	private long m_duration;

	//延迟结束一个周期的时间，默认都是3分钟
	private long m_extraTime;

	//提前启动一个周期的时间，默认都是3分钟
	private long m_aheadTime;

	private long m_lastStartTime;

	private long m_lastEndTime;

	public PeriodStrategy(long duration, long extraTime, long aheadTime) {
		m_duration = duration;
		m_extraTime = extraTime;
		m_aheadTime = aheadTime;
		m_lastStartTime = -1;
		m_lastEndTime = 0;
	}

	public long getDuration() {
		return m_duration;
	}

	public long next(long now) {
		long startTime = now - now % m_duration;

		//下一个period
		// for current period
		if (startTime > m_lastStartTime) {
			m_lastStartTime = startTime;
			return startTime;
		}

		//为下一个period做准备
		// prepare next period ahead
		if (now - m_lastStartTime >= m_duration - m_aheadTime) {
			m_lastStartTime = startTime + m_duration;
			return startTime + m_duration;
		}

		//更新 关闭上一个period的时间，返回-上上个period的时间
		// last period is over
		if (now - m_lastEndTime >= m_duration + m_extraTime) {
			long lastEndTime = m_lastEndTime;
			m_lastEndTime = startTime;
			return -lastEndTime;
		}

		return 0;
	}
}