package br.com.betogontijo.sbgindexer;

import java.util.Map;
import java.util.Set;

public class Trie {

	private SbgTrieNode root;
	private int size;

	public Trie() {
		root = new SbgTrieNode();
		size = 0;
	}

	public boolean add(String word) {
		SbgTrieNode trie = root;
		if (trie == null || word == null)
			return false;

		char[] chars = word.toCharArray();
		int counter = 0;
		while (counter < chars.length) {
			Set<String> childs = trie.getChildren().keySet();
			if (!childs.contains(chars[counter] + "")) {
				insertChar(trie, chars[counter] + "");
				if (counter == chars.length - 1) {
					getChild(trie, chars[counter]+"").setIsWord(true);
					size++;
					return true;
				}
			}
			trie = getChild(trie, chars[counter]+"");
			if (trie.getText().equals(word) && !trie.isWord()) {
				trie.setIsWord(true);
				size++;
				return true;
			}
			counter++;
		}
		return false;
	}

	public boolean find(String str) {
		Map<String, SbgTrieNode> children = root.getChildren();
		SbgTrieNode t = null;
		for (int i = 0; i < str.length(); i++) {
			char c = str.charAt(i);
			if (children.containsKey(c)) {
				t = children.get(c);
				children = t.getChildren();
			} else
				return false;
		}

		return true;
	}

	public boolean remove(String str) {
		return findNode(root, str);
	}

	private SbgTrieNode getChild(SbgTrieNode trie, String c) {
		return trie.getChildren().get(c);
	}

	private SbgTrieNode insertChar(SbgTrieNode trie, String c) {
		if (trie.getChildren().containsKey(c)) {
			return null;
		}

		SbgTrieNode next = new SbgTrieNode(trie.getText() + c.toString());
		trie.getChildren().put(c, next);
		return next;
	}

	private boolean findNode(SbgTrieNode trie, String s) {
		Map<String, SbgTrieNode> children = root.getChildren();

		SbgTrieNode parent = null;
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			if (children.containsKey(c)) {
				parent = trie;
				trie = children.get(c);
				children = trie.getChildren();
				if (trie.getText().equals(s)) {
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
