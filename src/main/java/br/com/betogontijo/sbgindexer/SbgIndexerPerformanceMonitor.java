package br.com.betogontijo.sbgindexer;

public class SbgIndexerPerformanceMonitor extends Thread {

	SbgIndexerDao dataSource;

	private volatile boolean running = true;

	private static final int printDelay = 1;

	SbgIndexerPerformanceMonitor(SbgIndexerDao dataSource) {
		this.dataSource = dataSource;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Thread#run()
	 */
	@Override
	public void run() {
		double overallRate = 0;
		double instantRate = 0;
		double currentTime = 0;
		int initialSize = 0;
		int atualSize = initialSize;
		int delayInMillis = printDelay * 1000;
		int lastSize;
		while (running) {
			try {
				lastSize = atualSize;
				atualSize = dataSource.getDocIdCounter();
				overallRate = (atualSize - initialSize) / currentTime;
				instantRate = (atualSize - lastSize) / printDelay;
				System.out.printf("OverallRate: %.2fDoc/s, InstantRate: %.2fDoc/s, TotalDocs: %d\r", overallRate,
						instantRate, atualSize);
				Thread.sleep(delayInMillis);
				currentTime += printDelay;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 
	 */
	public void cancel() {
		running = false;
	}
}
