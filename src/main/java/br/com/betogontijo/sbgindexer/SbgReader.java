package br.com.betogontijo.sbgindexer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import br.com.betogontijo.sbgcrawler.SbgDocument;

public class SbgReader {

	SbgDataSource dataSource = new SbgDataSource();

	AtomicInteger docId = new AtomicInteger();

	@SuppressWarnings("unchecked")
	public void read() {
		SbgDocument document;
		while ((document = dataSource.getDocument(docId.incrementAndGet())) != null) {
			Map<String, List<Integer>> wordMap = (Map<String, List<Integer>>) document.get("wordMap");
			Trie trie = new Trie();
			for (String word : wordMap.keySet()) {
				trie.add(word);
			}
			dataSource.addWord(trie.getRoot());
		}
	}
}
