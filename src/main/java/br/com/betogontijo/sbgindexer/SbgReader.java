package br.com.betogontijo.sbgindexer;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import br.com.betogontijo.sbgcrawler.SbgDocument;

public class SbgReader {

	SbgDataSource dataSource = new SbgDataSource();

	AtomicInteger docId = new AtomicInteger();

	Trie trie = new Trie();

	public void read() {
		SbgDocument document;
		while ((document = dataSource.getDocument(docId.incrementAndGet())) != null) {
			Map<String, int[]> wordMap = document.getWordsMap();
			for (String word : wordMap.keySet()) {
				trie.add(word, docId.get(), wordMap.get(word));
			}
		}
	}

	public Trie getTrie() {
		return this.trie;
	}
}
