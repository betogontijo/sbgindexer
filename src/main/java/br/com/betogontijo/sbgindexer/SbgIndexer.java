package br.com.betogontijo.sbgindexer;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

import br.com.betogontijo.sbgbeans.crawler.documents.SbgDocument;
import br.com.betogontijo.sbgbeans.indexer.documents.Node;

public class SbgIndexer implements Runnable {

	SbgIndexerDao dataSource;

	private boolean canceled;

	CountDownLatch latch;

	public SbgIndexer(SbgIndexerDao dataSource, CountDownLatch latch) {
		this.dataSource = dataSource;
		canceled = false;
		this.latch = latch;
	}

	public void index(SbgDocument document) {
		Map<String, int[]> wordsMap = new HashMap<String, int[]>();
		int pos = 0;
		for (String word : document.getWordsList()) {
			if (!word.isEmpty()) {
				IntList positions = new IntList();
				if (wordsMap.get(word) != null) {
					positions.addAll(wordsMap.get(word));
				}
				positions.add(pos++);
				wordsMap.put(word, positions.toArray());
			}
		}
		for (Entry<String,int[]> wordMap : wordsMap.entrySet()) {
			Node node = new Node();
			node.setWord(wordMap.getKey());
			Map<Integer, int[]> invertedList = new HashMap<Integer, int[]>();
			invertedList.put(document.getId(), wordMap.getValue());
			node.setInvertedList(invertedList);
			dataSource.addWord(node);
		}
	}

	@Override
	public void run() {
		SbgDocument document = null;
		while ((document = dataSource.getNextDocument()) != null && !isCanceled()) {
			index(document);
		}
		latch.countDown();
	}

	public boolean isCanceled() {
		return canceled;
	}

	public void setCanceled(boolean canceled) {
		this.canceled = canceled;
	}

}
