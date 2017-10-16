package br.com.betogontijo.sbgindexer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Trie {

	private SbgTrieNode root;
	private int size;

	public Trie() {
		root = new SbgTrieNode();
		size = 0;
	}

	public boolean add(String word, int docId, int[] docPositions) {
		SbgTrieNode trie = root;
		if (trie == null || word == null)
			return false;

		char[] chars = word.toCharArray();
		int counter = 0;
		while (counter < chars.length) {
			if (trie.getChildren() == null) {
				trie.setChildren(new HashMap<Character, SbgTrieNode>());
				insertChar(trie, chars[counter]);
			} else {
				Set<Character> childs = trie.getChildren().keySet();
				if (!childs.contains(chars[counter])) {
					insertChar(trie, chars[counter]);
				}
			}
			trie = getChild(trie, chars[counter]);
			if (counter == chars.length - 1) {
				List<int[]> invertedList = new ArrayList<int[]>();
				int[] docIdTmp = new int[1];
				docIdTmp[0] = docId;
				invertedList.add(docIdTmp);
				invertedList.add(docPositions);
				trie.setInvertedList(invertedList);
				size++;
				return true;
			}
			counter++;
		}
		return false;
	}

	public List<int[]> find(String str) {
		Map<Character, SbgTrieNode> children = root.getChildren();
		SbgTrieNode t = null;
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

	private SbgTrieNode getChild(SbgTrieNode trie, Character c) {
		return trie.getChildren().get(c);
	}

	private void insertChar(SbgTrieNode trie, Character c) {
		Map<Character, SbgTrieNode> children = trie.getChildren();
		SbgTrieNode next = null;
		if (children == null) {
			children = new HashMap<Character, SbgTrieNode>();
			next = new SbgTrieNode();
			trie.setChildren(children);
		} else {
			next = trie.getChildren().get(c);
			if (next == null) {
				next = new SbgTrieNode();
			}
		}
		trie.getChildren().put(c, next);
	}

	private boolean findNode(SbgTrieNode trie, String s) {
		Map<Character, SbgTrieNode> children = root.getChildren();

		SbgTrieNode parent = null;
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

	public SbgTrieNode getRoot() {
		return root;
	}
}
