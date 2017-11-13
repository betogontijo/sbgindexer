package br.com.betogontijo.sbgindexer;

import java.util.Map;

import br.com.betogontijo.sbgbeans.crawler.documents.SbgDocument;
import br.com.betogontijo.sbgbeans.indexer.documents.Node;

public class SbgIndexer implements Runnable {

	SbgIndexerDao dataSource;

	private boolean canceled;

	public SbgIndexer(SbgIndexerDao dataSource) {
		this.dataSource = dataSource;
		canceled = false;
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
				dataSource.addWord(node);
			}
		}
		setCanceled(true);
	}

	@Override
	public void run() {
		try {
			index();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public boolean isCanceled() {
		return canceled;
	}

	public void setCanceled(boolean canceled) {
		this.canceled = canceled;
	}

}
