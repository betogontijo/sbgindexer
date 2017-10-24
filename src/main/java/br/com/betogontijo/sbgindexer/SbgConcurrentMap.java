package br.com.betogontijo.sbgindexer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import br.com.betogontijo.sbgbeans.indexer.documents.Node;

public class SbgConcurrentMap extends ConcurrentHashMap<String, Node> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -692268726116190502L;

	final java.util.concurrent.locks.ReentrantLock removingLock = new ReentrantLock();

	final java.util.concurrent.locks.ReentrantLock insertingLock = new ReentrantLock();

	public void concurrentAdd(String key, List<Integer> integerList, List<int[]> arrayList) {
		insertingLock.lock();
		Node node = (Node) get(key);
		if (node == null) {
			node = new Node();
			node.setWord(key);
			put(key, node);
		}
		node.getDocRefList().addAll(integerList);
		node.getOccurrencesList().addAll(arrayList);
		insertingLock.unlock();
	}

	public List<Node> removeMany(int count) {
		List<Node> nextList = new ArrayList<Node>();
		removingLock.lock();
		try {
			while (count > 0) {
				Node next = peek();
				nextList.add(next);
				remove(next);
				count--;
			}
			return nextList;
		} finally {
			removingLock.unlock();
		}
	}

	public Node peek() {
		try {
			return get(element());
		} catch (NoSuchElementException e) {
			return null;
		}
	}

	public String element() {
		return iterator().next();
	}

	public Iterator<String> iterator() {
		return keySet().iterator();
	}
}
