package br.com.betogontijo.sbgindexer;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.springframework.dao.DataIntegrityViolationException;

import br.com.betogontijo.sbgbeans.crawler.documents.SbgDocument;
import br.com.betogontijo.sbgbeans.crawler.repositories.SbgDocumentRepository;
import br.com.betogontijo.sbgbeans.indexer.documents.Node;
import br.com.betogontijo.sbgbeans.indexer.repositories.NodeRepository;

public class SbgIndexerDao {

	AtomicLong uncompressedIndex = new AtomicLong();

	AtomicLong compressedIndex = new AtomicLong();

	int documentIdCounter = 3378;

	SbgDocumentRepository documentRepository;

	NodeRepository nodeRepository;

	Iterator<SbgDocument> documentsIterator;

	final ReentrantLock lock = new ReentrantLock();

	public SbgIndexerDao(int threadNumber, int bufferSize, SbgDocumentRepository documentRepository,
			NodeRepository nodeRepository) {
		this.documentRepository = documentRepository;
		this.nodeRepository = nodeRepository;
		// documentIdCounter = nodeRepository.getCurrentDocumentsIndexed();
		documentsIterator = documentRepository.iterator(documentIdCounter);
	}

	SbgDocument getNextDocument() {
		try {
			documentIdCounter++;
			lock.lock();
			return documentsIterator.next();
		} catch (NoSuchElementException e) {
			return null;
		} finally {
			lock.unlock();
		}
	}

	void addWord(Node node) {
		node.compress();
		try {
			nodeRepository.upsertNode(node);
		} catch (DataIntegrityViolationException e) {
			// Too much caracteres in word
		}
	}

	public boolean saveIndexedDocumentsNumber() {
		return nodeRepository.saveDocumentsIndexed(getDocIdCounter());
	}

	public int getDocIdCounter() {
		return documentIdCounter;
	}
}
