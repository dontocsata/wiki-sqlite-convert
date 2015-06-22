package com.dontocsata.offlinewiki;

public class Page {

	private int id;
	private String title;
	private String redirect;
	private PageNamespace ns;
	private String content;
	private byte[] bytes;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public PageNamespace getNs() {
		return ns;
	}

	public void setNs(PageNamespace ns) {
		this.ns = ns;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public String getRedirect() {
		return redirect;
	}

	public void setRedirect(String redirect) {
		this.redirect = redirect;
	}

	public byte[] getBytes() {
		return bytes;
	}

	public void setBytes(byte[] bytes) {
		this.bytes = bytes;
	}

}
