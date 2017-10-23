package br.com.betogontijo.sbgindexer;

import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
@EnableMongoRepositories("br.com.betogontijo.sbgbeans")
public class SbgIndexerMain {

	SbgIndexerDao dataSource;

	@Autowired
	SbgDocumentRepository documentRepository;

	@Autowired
	NodeRepository nodeRepository;

	public static void main(String[] args) {
		SpringApplication.run(SbgIndexerMain.class, args);
	}

	@SuppressWarnings("deprecation")
	@Bean
	CommandLineRunner init(ConfigurableApplicationContext applitcationContext) {
		return args -> {
			dataSource = new SbgIndexerDao(documentRepository, nodeRepository);
			SbgIndexerPerformanceMonitor monitor = new SbgIndexerPerformanceMonitor(dataSource);
			monitor.start();
			SbgIndexer indexer = new SbgIndexer(dataSource);
			Properties properties = new Properties();
			properties.load(ClassLoader.getSystemResourceAsStream("sbgindexer.properties"));
			int threadNumber = Integer.parseInt(properties.getProperty("environment.threads"));
			ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(threadNumber, threadNumber, 0L,
					TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
			while (!dataSource.isCanceled()) {
				if (threadPoolExecutor.getActiveCount() < threadNumber) {
					threadPoolExecutor.execute(indexer);
				}
			}
			monitor.stop();
		};

	}

}
