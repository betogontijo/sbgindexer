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

	private SbgConcurrentMap nodeBufferMap;

	private static int bufferSize;

	private static int bufferPerThread;

	private boolean canceled = false;

	public SbgIndexerDao(int threadNumber, int bufferSize, SbgDocumentRepository documentRepository,
			NodeRepository nodeRepository) {
		setBufferSize(bufferSize);
		setBufferPerThread(bufferSize / threadNumber);
		this.documentRepository = documentRepository;
		this.nodeRepository = nodeRepository;
		documentIdCounter = new AtomicInteger((int) nodeRepository.count());
		setNodeBufferMap(new SbgConcurrentMap());
	}

	SbgDocument getNextDocument() {
		SbgDocument findById = documentRepository.findById(documentIdCounter.getAndIncrement());
		if (findById == null) {
			setCanceled(true);
		}
		return findById;
	}

	public void insertWord(Node node) {
		getNodeBufferMap().concurrentAdd(node.getWord(), node.getDocRefList(), node.getOccurrencesList());
		if (getNodeBufferMap().size() > getBufferSize()) {
			try {
				nodeRepository.insertAllNodes(getNodeBufferMap().removeMany(getBufferPerThread()));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
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

	public SbgConcurrentMap getNodeBufferMap() {
		return nodeBufferMap;
	}

	public void setNodeBufferMap(SbgConcurrentMap nodeBufferMap) {
		this.nodeBufferMap = nodeBufferMap;
	}

	public static int getBufferSize() {
		return bufferSize;
	}

	public static void setBufferSize(int bufferSize) {
		SbgIndexerDao.bufferSize = bufferSize;
	}

	public static int getBufferPerThread() {
		return bufferPerThread;
	}

	public static void setBufferPerThread(int bufferPerThread) {
		SbgIndexerDao.bufferPerThread = bufferPerThread;
	}
}
