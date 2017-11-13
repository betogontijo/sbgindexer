package br.com.betogontijo.sbgindexer;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

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
		return findById;
	}

	void addWord(Node node) {
		Node findByWord = nodeRepository.findByWord(node.getWord());
		boolean insertNode = true;
		if (findByWord != null) {
			Integer docId = node.getDocRefList().iterator().next();
			int[] occurrences = node.getOccurrencesList().iterator().next();
			if (findByWord.getDocRefList().add(docId)) {
				findByWord.getOccurrencesList().add(occurrences);
			}
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

	public void insertWord(Node node) {
		concurrentAdd(node.getWord(), node.getDocRefList(), node.getOccurrencesList());
		if (getNodeBufferMap().size() > getBufferSize()) {
			try {
				nodeRepository.insertAllNodes(getNodeBufferMap().removeMany(getBufferPerThread()));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	final java.util.concurrent.locks.ReentrantLock insertingLock = new ReentrantLock();

	public void concurrentAdd(String key, Set<Integer> integerList, Set<int[]> arrayList) {
		insertingLock.lock();
		Node node = (Node) getNodeBufferMap().get(key);
		if (node == null) {
			node = new Node();
			node.setWord(key);
			getNodeBufferMap().put(key, node);
		}
		node.getDocRefList().addAll(integerList);
		node.getOccurrencesList().addAll(arrayList);
		insertingLock.unlock();
	}

	public int getDocIdCounter() {
		return documentIdCounter.get();
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
