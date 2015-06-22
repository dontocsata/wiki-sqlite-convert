package com.dontocsata.offlinewiki;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Codes from: https://en.wikipedia.org/wiki/Wikipedia:Namespace
 * 
 * @author ray
 */
public enum PageNamespace {
	ARTICLE(0),
	ARTICLE_TALK(1, true),
	USER(2),
	USER_TALK(3, true),
	WIKIPEDIA(4),
	WIKIPEDIA_TALK(5, true),
	FILE(6),
	FILE_TALK(7, true),
	MEDIA_WIKI(8),
	MEDIA_WIKI_TALK(9, true),
	TEMPLATE(10),
	TEMPLATE_TALK(11, true),
	HELP(12),
	HELP_TALK(13, true),
	CATEGORY(14),
	CATEGORY_TALK(15, true),
	PORTAL(100),
	PORTAL_TALK(101, true),
	BOOK(108),
	BOOK_TALK(109, true),
	DRAFT(118),
	DRAFT_TALK(119, true),
	EDUCATION_PROGRAM(446),
	EDUCATION_PROGRAM_TALK(447, true),
	TIMED_TEXT(710),
	TIMED_TEXT_TALK(711, true),
	MODULE(828),
	MODULE_TALK(829, true),
	TOPIC(2600),
	SPECIAL(-1),
	MEDIA(-2);

	private static final Map<Integer, PageNamespace> map = new HashMap<>();
	static {
		Arrays.asList(values()).forEach(ns -> map.put(ns.code, ns));
	}

	private int code;
	private boolean isTalkPage;

	private PageNamespace(int code) {
		this(code, false);
	}

	private PageNamespace(int code, boolean isTalkPage) {
		this.code = code;
		this.isTalkPage = isTalkPage;
	}

	public boolean isTalkPage() {
		return isTalkPage;
	}

	public static PageNamespace getFromCode(int code) {
		return map.get(code);
	}
}
