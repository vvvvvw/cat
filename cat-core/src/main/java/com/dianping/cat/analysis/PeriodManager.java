package com.dianping.cat.analysis;

import java.util.ArrayList;
import java.util.List;

import org.unidal.helper.Threads;
import org.unidal.helper.Threads.Task;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.logging.Logger;

import com.dianping.cat.Cat;
import com.dianping.cat.analysis.MessageAnalyzerManager;
import com.dianping.cat.analysis.PeriodStrategy;
import com.dianping.cat.statistic.ServerStatisticManager;

public class PeriodManager implements Task {
	private PeriodStrategy m_strategy;

	private List<Period> m_periods = new ArrayList<Period>();

	private boolean m_active;

	@Inject
	private MessageAnalyzerManager m_analyzerManager;

	@Inject
	private ServerStatisticManager m_serverStateManager;

	@Inject
	private Logger m_logger;

	public static long EXTRATIME = 3 * 60 * 1000L;

	public PeriodManager(long duration, MessageAnalyzerManager analyzerManager,
	      ServerStatisticManager serverStateManager, Logger logger) {
		m_strategy = new PeriodStrategy(duration, EXTRATIME, EXTRATIME);
		m_active = true;
		m_analyzerManager = analyzerManager;
		m_serverStateManager = serverStateManager;
		m_logger = logger;
	}

	private void endPeriod(long startTime) {
		int len = m_periods.size();

		for (int i = 0; i < len; i++) {
			Period period = m_periods.get(i);

			if (period.isIn(startTime)) {
				period.finish();
				//调用PeriodManager的endPeriod(long startTime)方法完成周期的清理工作，
				//然后将period从m_periods列表移除出去
				m_periods.remove(i);
				break;
			}
		}
	}

	public Period findPeriod(long timestamp) {
		for (Period period : m_periods) {
			if (period.isIn(timestamp)) {
				return period;
			}
		}

		return null;
	}

	@Override
	public String getName() {
		return "RealtimeConsumer-PeriodManager";
	}

	public void init() {
		long startTime = m_strategy.next(System.currentTimeMillis());

		startPeriod(startTime);
	}

	//每隔1秒钟会计算是否需要开启一个新的周期，value>0就开启新的周期，
	// value=0啥也不干，value<0的异步开启一个新线程结束上一个周期
	@Override
	public void run() {
		while (m_active) {
			try {
				long now = System.currentTimeMillis();
				long value = m_strategy.next(now);

				if (value > 0) {
					//value>0就开启新的周期
					startPeriod(value);
				} else if (value < 0) {
					//value<0的异步开启一个新线程结束上一个周期。
					//结束线程调用PeriodManager的endPeriod(long startTime)方法完成周期的清理工作，
					// 然后将period从m_periods列表移除出去
					// last period is over,make it asynchronous
					Threads.forGroup("cat").start(new EndTaskThread(-value));
				}
			} catch (Throwable e) {
				Cat.logError(e);
			}

			try {
				//每隔1秒钟会计算是否需要开启一个新的周期
				Thread.sleep(1000L);
			} catch (InterruptedException e) {
				break;
			}
		}
	}

	@Override
	public void shutdown() {
		m_active = false;
	}

	private void startPeriod(long startTime) {
		long endTime = startTime + m_strategy.getDuration();
		Period period = new Period(startTime, endTime, m_analyzerManager, m_serverStateManager, m_logger);

		m_periods.add(period);
		period.start();
	}

	private class EndTaskThread implements Task {

		private long m_startTime;

		public EndTaskThread(long startTime) {
			m_startTime = startTime;
		}

		@Override
		public String getName() {
			return "End-Consumer-Task";
		}

		@Override
		public void run() {
			endPeriod(m_startTime);
		}

		@Override
		public void shutdown() {
		}
	}
}