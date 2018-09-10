package com.dianping.cat.analysis;

import com.dianping.cat.message.spi.MessageQueue;
import com.dianping.cat.report.ReportManager;

public interface MessageAnalyzer {

	public void analyze(MessageQueue queue);

	public void destroy();

	public void doCheckpoint(boolean atEnd);

	public long getStartTime();

	public void initialize(long startTime, long duration, long extraTime);

	//每种类别分析器具体有多少个实例
	public int getAnalyzerCount();

	public void setIndex(int index);

	public ReportManager<?> getReportManager();
}
