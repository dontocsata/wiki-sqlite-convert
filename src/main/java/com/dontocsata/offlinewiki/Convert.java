package com.dontocsata.offlinewiki;

import java.io.File;

public class Convert {

	public static void main(String[] args) throws Exception {
		File file = new File(args[0]);
		WikiConverter converter = new WikiConverter(new File("output/"), false, true, 4);
		long time = System.currentTimeMillis();

		ProgressCallBack callback = new ProgressCallBack() {

			private long time = System.currentTimeMillis();

			@Override
			public void callback(String title, int totalCount) {
				long newTime = System.currentTimeMillis();
				System.out.println("Count=" + totalCount + ", Title=" + title + ", Took " + (newTime - time) / 1000.0
						+ "s");
				time = newTime;

			}
		};
		int articles = Integer.MAX_VALUE;
		converter.convert(file, articles, callback, 10000);
		time = System.currentTimeMillis() - time;
		System.out.println("Took " + time / 1000.0 + "s");
	}

}
