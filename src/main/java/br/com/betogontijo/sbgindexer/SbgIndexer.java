package br.com.betogontijo.sbgindexer;

import java.util.Map;

import br.com.betogontijo.sbgbeans.crawler.documents.SbgDocument;
import br.com.betogontijo.sbgbeans.indexer.documents.Node;

public class SbgIndexer implements Runnable {

	SbgIndexerDao dataSource;

	public SbgIndexer(SbgIndexerDao dataSource) {
		this.dataSource = dataSource;
	}

	public void index() {
		SbgDocument document;
		while ((document = dataSource.getNextDocument()) != null) {
			Map<String, int[]> wordMap = document.getWordsMap();
			for (String word : wordMap.keySet()) {
				Node node = new Node();
				node.setWord(word);
				node.getDocRefList().add(document.getId());
				node.getOccurrencesList().add(wordMap.get(word));
				dataSource.insertWord(node);
			}
		}
	}

	@Override
	public void run() {
		try {
			index();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
