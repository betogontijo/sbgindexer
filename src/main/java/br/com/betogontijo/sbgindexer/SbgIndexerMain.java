package br.com.betogontijo.sbgindexer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

import br.com.betogontijo.sbgbeans.crawler.documents.SbgDocument;
import br.com.betogontijo.sbgbeans.crawler.repositories.SbgDocumentRepository;
import br.com.betogontijo.sbgbeans.indexer.documents.InvertedList;
import br.com.betogontijo.sbgbeans.indexer.repositories.NodeRepository;

@SpringBootApplication
@EnableMongoRepositories("br.com.betogontijo.sbgbeans")
public class SbgIndexerMain {

	SbgDataSource dataSource;

	AtomicInteger docId = new AtomicInteger();

	Trie trie = new Trie();

	@Autowired
	SbgDocumentRepository documentRepository;

	@Autowired
	NodeRepository nodeRepository;

	public static void main(String[] args) {
		SpringApplication.run(SbgIndexerMain.class, args);
	}

	@Bean
	CommandLineRunner init(ConfigurableApplicationContext applitcationContext) {
		return args -> {
			dataSource = new SbgDataSource(documentRepository, nodeRepository);
			read();
			InvertedList invertedList = trie.find("batata");
			List<Integer> docRefList = invertedList.getDocRefList();
			List<int[]> occurrencesList = invertedList.getOccurrencesList();
			String output = "{";
			for (int i = 0; i < docRefList.size(); i++) {
				output += docRefList.get(i) + ",[";
				for (int j = 0; j < occurrencesList.get(i).length; j++) {
					output += occurrencesList.get(i)[j] + ",";
				}
				output = output.substring(0, output.length() - 1) + "],";
			}
			output = output.substring(0, output.length() - 1) + "}";
			System.out.println(output);
		};

	}

	public void read() {
		SbgDocument document;
		while ((document = dataSource.getDocument(docId.incrementAndGet())) != null) {
			Map<String, int[]> wordMap = document.getWordsMap();
			for (String word : wordMap.keySet()) {
				trie.add(word, docId.get(), wordMap.get(word));
			}
		}
		dataSource.addWord(trie.getRoot());
	}

}
