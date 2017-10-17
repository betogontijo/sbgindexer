package br.com.betogontijo.sbgindexer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import br.com.betogontijo.sbgbeans.indexer.documents.InvertedList;
import br.com.betogontijo.sbgbeans.indexer.documents.Node;

public class Trie {

	private Node root;
	private int size;

	public Trie() {
		root = new Node();
		size = 0;
	}

	public boolean add(String word, int docId, int[] docPositions) {
		Node trie = root;
		if (trie == null || word == null)
			return false;

		char[] chars = word.toCharArray();
		int counter = 0;
		while (counter < chars.length) {
			if (trie.getChildren() == null) {
				trie.setChildren(new HashMap<Character, Node>());
				insertChar(trie, chars[counter]);
			} else {
				Set<Character> childs = trie.getChildren().keySet();
				if (!childs.contains(chars[counter])) {
					insertChar(trie, chars[counter]);
				}
			}
			trie = getChild(trie, chars[counter]);
			if (counter == chars.length - 1) {
				InvertedList invertedList = new InvertedList();
				invertedList.addInvertedItem(docId, docPositions);
				trie.setInvertedList(invertedList);
				size++;
				return true;
			}
			counter++;
		}
		return false;
	}

	public InvertedList find(String str) {
		Map<Character, Node> children = root.getChildren();
		Node t = null;
		for (int i = 0; i < str.length(); i++) {
			char c = str.charAt(i);
			if (children.containsKey(c)) {
				t = children.get(c);
				if (i == str.length() - 1) {
					return t.getInvertedList();
				} else {
					children = t.getChildren();
				}
			} else {
				break;
			}
		}
		return null;
	}

	public boolean remove(String str) {
		return findNode(root, str);
	}

	private Node getChild(Node trie, Character c) {
		return trie.getChildren().get(c);
	}

	private void insertChar(Node trie, Character c) {
		Map<Character, Node> children = trie.getChildren();
		Node next = null;
		if (children == null) {
			children = new HashMap<Character, Node>();
			next = new Node();
			trie.setChildren(children);
		} else {
			next = trie.getChildren().get(c);
			if (next == null) {
				next = new Node();
			}
		}
		trie.getChildren().put(c, next);
	}

	private boolean findNode(Node trie, String s) {
		Map<Character, Node> children = root.getChildren();

		Node parent = null;
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			if (children.containsKey(c)) {
				parent = trie;
				trie = children.get(c);
				children = trie.getChildren();
				if (trie.equals(s)) {
					parent.getChildren().remove(c);
					trie = null;
					return true;
				}
			}
		}
		return false;
	}

	public int getSize() {
		return size;
	}

	public Node getRoot() {
		return root;
	}
}
