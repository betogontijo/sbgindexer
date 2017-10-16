package br.com.betogontijo.sbgindexer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import me.lemire.integercompression.differential.IntegratedIntCompressor;

public class SbgTrieNode {

	private Map<Character, SbgTrieNode> children;
	private List<int[]> invertedList;

	SbgTrieNode() {
		invertedList = new ArrayList<int[]>();
	}

	public Map<Character, SbgTrieNode> getChildren() {
		return this.children;
	}

	public void setChildren(Map<Character, SbgTrieNode> children) {
		this.children = children;
	}

	public List<int[]> getInvertedList() {
		IntegratedIntCompressor iic = new IntegratedIntCompressor();
		List<int[]> invertedList = new ArrayList<int[]>();
		for (int i = 0; i < this.invertedList.size(); i++) {
			if (i % 2 == 0) {
				invertedList.add(this.invertedList.get(i));
			} else {
				invertedList.add(iic.uncompress(this.invertedList.get(i)));
			}
		}
		return invertedList;
	}

	public void setInvertedList(List<int[]> invertedList) {
		IntegratedIntCompressor iic = new IntegratedIntCompressor();
		for (int i = 0; i < invertedList.size(); i++) {
			if (i % 2 == 0) {
				this.invertedList.add(invertedList.get(i));
			} else {
				this.invertedList.add(iic.compress(invertedList.get(i)));
			}
		}
	}
}
