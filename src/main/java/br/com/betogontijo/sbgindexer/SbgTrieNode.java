package br.com.betogontijo.sbgindexer;

import br.com.betogontijo.sbgcrawler.SbgMap;

public class SbgTrieNode extends SbgMap<String, Object> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2463008879043012732L;
	// private HashMap<Character, SbgTrieNode> children;
	// private String text;
	// private boolean isWord;

	public SbgTrieNode() {
		setChildren(new SbgMap<String, SbgTrieNode>());
		setText("");
		setIsWord(false);
	}
	
	public SbgTrieNode(boolean isSearch){
		if(isSearch){
			setText("");
		}
	}

	public SbgTrieNode(String text) {
		this();
		setText(text);
	}

	@SuppressWarnings("unchecked")
	public SbgMap<String, SbgTrieNode> getChildren() {
		return (SbgMap<String, SbgTrieNode>) get("children");
	}

	private void setChildren(SbgMap<String, SbgTrieNode> children) {
		put("children", children);
	}

	public String getText() {
		return (String) get("text");
	}

	private void setText(String text) {
		put("text", text);
	}

	public boolean isWord() {
		return (Boolean) get("isWord");
	}

	public void setIsWord(boolean isWord) {
		put("isWord", isWord);
	}

	@Override
	public String toString() {
		return getText();
	}
}
