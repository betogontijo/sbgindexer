package br.com.betogontijo.sbgindexer;

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

	private boolean canceled = false;

	SbgIndexerDao(SbgDocumentRepository documentRepository, NodeRepository nodeRepository) {
		this.documentRepository = documentRepository;
		this.nodeRepository = nodeRepository;
		documentIdCounter = new AtomicInteger((int) nodeRepository.count());
	}

	SbgDocument getNextDocument() {
		SbgDocument findById = documentRepository.findById(documentIdCounter.getAndIncrement());
		if (findById == null) {
			setCanceled(true);
		}
		return findById;
	}

	void addWord(Node node) {
		Node findByWord = nodeRepository.findByWord(node.getWord());
		boolean insertNode = true;
		if (findByWord != null) {
			findByWord.getDocRefList().addAll(node.getDocRefList());
			findByWord.getOccurrencesList().addAll(node.getOccurrencesList());
			node = findByWord;
			insertNode = false;
		}
		// uncompressedIndex.getAndAdd(node.size());
		// node.getInvertedList().compress();
		if (insertNode) {
			nodeRepository.upsertNode(node);
		} else {
			nodeRepository.updateNode(node);
		}
		// compressedIndex.getAndAdd(node.size());
	}

	public int getDocIdCounter() {
		return documentIdCounter.get();
	}

	public boolean isCanceled() {
		return canceled;
	}

	public void setCanceled(boolean canceled) {
		this.canceled = canceled;
	}

	public double getCompressRatio() {
		double ratio = 0;
		try {
			ratio = compressedIndex.get() / uncompressedIndex.get();
		} catch (ArithmeticException e) {
		}
		return ratio;
	}
}
