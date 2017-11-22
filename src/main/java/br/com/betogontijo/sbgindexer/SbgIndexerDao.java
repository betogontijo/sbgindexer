package br.com.betogontijo.sbgindexer;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import br.com.betogontijo.sbgbeans.crawler.documents.SbgDocument;
import br.com.betogontijo.sbgbeans.crawler.repositories.SbgDocumentRepository;
import br.com.betogontijo.sbgbeans.indexer.documents.Node;
import br.com.betogontijo.sbgbeans.indexer.repositories.NodeRepository;

public class SbgIndexerDao {

	AtomicLong uncompressedIndex = new AtomicLong();

	AtomicLong compressedIndex = new AtomicLong();

	AtomicInteger documentIdCounter;

	SbgDocumentRepository documentRepository;

	NodeRepository nodeRepository;

	Iterator<SbgDocument> documentsIterator;

	public SbgIndexerDao(int threadNumber, int bufferSize, SbgDocumentRepository documentRepository,
			NodeRepository nodeRepository) {
		this.documentRepository = documentRepository;
		this.nodeRepository = nodeRepository;
		documentIdCounter = new AtomicInteger((int) nodeRepository.count());
		documentsIterator = documentRepository.findAll().iterator();
		for (int i = 0; i < documentIdCounter.get(); i++)
			documentsIterator.next();
	}

	SbgDocument getNextDocument() {
		try {
			documentIdCounter.getAndIncrement();
			return documentsIterator.next();
		} catch (NoSuchElementException e) {
			return null;
		}
	}

	void addWord(Node node) {
		Node findByWord = nodeRepository.findByWord(node.getWord());
		if (findByWord != null) {
			for (Entry<Integer, int[]> entry : findByWord.getInvertedList().entrySet()) {
				node.getInvertedList().put(entry.getKey(), entry.getValue());
			}
			nodeRepository.updateNode(node);
		}
		// uncompressedIndex.getAndAdd(node.size());
		// node.getInvertedList().compress();
		else {
			nodeRepository.upsertNode(node);
		}
		// compressedIndex.getAndAdd(node.size());
	}

	public int getDocIdCounter() {
		return documentIdCounter.get();
	}
}
