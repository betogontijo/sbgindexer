package br.com.betogontijo.sbgindexer;

import java.util.Arrays;
import java.util.HashMap;
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
			Map<String, int[]> wordsMap = new HashMap<String, int[]>();
			int pos = 0;
			for (String word : document.getWordsList()) {
				if (!word.isEmpty()) {
					int[] positions;
					if (wordsMap.get(word) != null) {
						positions = wordsMap.get(word);
						positions = Arrays.copyOf(positions, positions.length + 1);
						positions[positions.length - 1] = pos++;
					} else {
						positions = new int[1];
						positions[0] = pos++;
					}
					wordsMap.put(word, positions);
				}
			}
			for (String word : wordsMap.keySet()) {
				Node node = new Node();
				node.setWord(word);
				Map<Integer, int[]> invertedList = new HashMap<Integer, int[]>();
				invertedList.put(document.getId(), wordsMap.get(word));
				node.setInvertedList(invertedList);
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
