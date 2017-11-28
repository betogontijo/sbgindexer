package br.com.betogontijo.sbgindexer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

import br.com.betogontijo.sbgbeans.crawler.repositories.SbgDocumentRepository;
import br.com.betogontijo.sbgbeans.indexer.repositories.NodeRepository;

@SpringBootApplication
@EnableMongoRepositories({ "br.com.betogontijo.sbgbeans.crawler", "br.com.betogontijo.sbgbeans.indexer" })
public class SbgIndexerMain {

	@Autowired
	SbgDocumentRepository documentRepository;

	@Autowired
	NodeRepository nodeRepository;

	SbgIndexerDao dataSource;

	SbgIndexer indexer;

	ThreadPoolExecutor threadPoolExecutor;

	SbgIndexerPerformanceMonitor monitor;

	public static void main(String[] args) {
		SpringApplication.run(SbgIndexerMain.class, args);
	}

	@Bean
	CommandLineRunner init(ConfigurableApplicationContext applitcationContext) {
		return args -> {
			Properties properties = new Properties();
			properties.load(ClassLoader.getSystemResourceAsStream("sbgindexer.properties"));
			int threadNumber = Integer.parseInt(properties.getProperty("environment.threads"));
			int bufferSize = Integer.parseInt(properties.getProperty("environment.buffer.size"));

			dataSource = new SbgIndexerDao(threadNumber, bufferSize, documentRepository, nodeRepository);

			monitor = new SbgIndexerPerformanceMonitor(dataSource);
			monitor.start();

			threadPoolExecutor = new ThreadPoolExecutor(threadNumber, threadNumber, 0L, TimeUnit.MILLISECONDS,
					new LinkedBlockingQueue<Runnable>());
			CountDownLatch latch = new CountDownLatch(threadNumber);

			indexer = new SbgIndexer(dataSource, latch);
			while (threadPoolExecutor.getActiveCount() < threadNumber) {
				threadPoolExecutor.execute(indexer);
			}
			latch.await();
			monitor.cancel();
		};

	}

	@PreDestroy
	public void onDestroy() {
		System.out.println("Waiting all indexers to end...");
		indexer.setCanceled(true);
		monitor.cancel();
		try {
			boolean awaitTermination = threadPoolExecutor.awaitTermination(1, TimeUnit.MINUTES);
			if (awaitTermination) {
				System.out.println("All collectors have finished.");
			} else {
				System.out.println("Collectors was forced finishing, timeout reached.");
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Shutting down...");
		boolean isSaved = dataSource.saveIndexedDocumentsNumber();
		System.out.println("Current count of indexed documents saved? (" + isSaved + ").");
		threadPoolExecutor.shutdown();
		System.out.println("Shutdown.");
	}

}
