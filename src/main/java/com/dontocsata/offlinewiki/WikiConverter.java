package com.dontocsata.offlinewiki;

import info.bliki.wiki.model.WikiModel;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class WikiConverter {

	public static final int MAX_PAGES = 300;

	private long maxFileSize = 1024L * 1024L * 1024L * 1024L;
	private boolean compress = true;
	private File outputDirectory;

	private Directory directory;
	private IndexWriter iwriter;
	private Connection conn;
	private int fileNumber = -1;

	private List<FileChannel> channels = new ArrayList<>();

	private EnumSet<PageNamespace> namespaces = EnumSet.of(PageNamespace.ARTICLE, PageNamespace.CATEGORY);

	private ExecutorService exec;

	public WikiConverter(File outputDirectory, boolean fullTextIndex, boolean compress, int threads)
			throws IOException, SQLException {
		this(outputDirectory, fullTextIndex, compress, threads, 1024L * 1024L * 1024L);
	}

	public WikiConverter(File outputDirectory, boolean fullTextIndex, boolean compress, int threads, long maxFileSize)
			throws IOException, SQLException {
		this.outputDirectory = outputDirectory;
		this.compress = compress;
		this.maxFileSize = maxFileSize;
		if (!outputDirectory.exists()) {
			outputDirectory.mkdirs();
		}
		if (!outputDirectory.isDirectory()) {
			throw new IllegalArgumentException(outputDirectory + " is not a directory.");
		}
		if (new File(outputDirectory, "meta.db").exists()) {
			throw new IllegalArgumentException("Output directory is not empty: " + outputDirectory);
		}
		if (fullTextIndex) {
			directory = FSDirectory.open(new File(outputDirectory, "fulltext").toPath());
			Analyzer analyzer = new StandardAnalyzer();
			IndexWriterConfig config = new IndexWriterConfig(analyzer);
			iwriter = new IndexWriter(directory, config);
		}
		conn = DriverManager.getConnection("jdbc:sqlite:" + new File(outputDirectory, "meta.db").getAbsolutePath());
		try (Statement stmt = conn.createStatement()) {
			stmt.execute("CREATE TABLE \"android_metadata\" (\"locale\" TEXT DEFAULT 'en_US')");
			stmt.executeUpdate("INSERT INTO \"android_metadata\" VALUES ('en_US')");
			stmt.execute("create virtual table titles using fts3(_id integer, title text);");
			stmt.execute("create table wiki (_id integer primary key, title text, redirect text, file integer, position integer, length integer);");
			stmt.execute("create table meta (key text primary key, value text);");
			stmt.executeUpdate("insert into meta VALUES ('compressed_values', '" + compress + "')");
		}
		exec = new ThreadPoolExecutor(threads, threads, 60L, TimeUnit.SECONDS, new LimitedQueue<Runnable>(64));
	}

	public void convert(File wiki, int maxArticleCount, ProgressCallBack callback, int howOften)
			throws XMLStreamException, IOException, InterruptedException, SQLException {
		if (!wiki.exists()) {
			throw new FileNotFoundException(wiki + " doesn't exist");
		}
		InputStream in = null;
		if (wiki.getName().endsWith("bz2")) {
			in = new BZip2CompressorInputStream(new BufferedInputStream(new FileInputStream(wiki)));
		} else {
			in = new BufferedInputStream(new FileInputStream(wiki));
		}
		XMLInputFactory xmlif = XMLInputFactory.newInstance();
		XMLStreamReader reader = xmlif.createXMLStreamReader(in);
		int count = 0;
		boolean inPage = false;
		boolean inId = false;
		boolean inTitle = false;
		boolean inValue = false;
		boolean inRedirect = false;
		boolean inNs = false;
		int id = -1;
		StringBuilder value = new StringBuilder();
		String title = null;

		boolean gotId = false;
		boolean gotTitle = false;
		boolean gotValue = false;
		boolean gotRedirect = false;
		Page page = new Page();

		List<Page> pages = new ArrayList<>();

		while (reader.hasNext() && count < maxArticleCount) {
			int eventType = reader.next();
			if (eventType == XMLStreamConstants.START_ELEMENT) {
				switch (reader.getLocalName()) {
					case "page":
						inPage = true;
						break;
					case "id":
						if (inPage && gotTitle && !gotId) {
							inId = true;
						}
						break;
					case "title":
						if (inPage && !gotTitle) {
							inTitle = true;
						}
						break;
					case "text":
						if (inPage) {
							inValue = true;
						}
						break;
					case "ns":
						if (inPage) {
							inNs = true;
						}
						break;
					case "redirect":
						if (inPage) {
							inRedirect = true;
						}
						for (int i = 0; i < reader.getAttributeCount(); i++) {
							if ("title".equals(reader.getAttributeLocalName(i))) {
								page.setRedirect(reader.getAttributeValue(i));
							}
						}
						break;
				}
			} else if (eventType == XMLStreamConstants.END_ELEMENT) {
				switch (reader.getLocalName()) {
					case "page":
						inPage = false;
						if (page.getNs() == null || namespaces.contains(page.getNs())) {
							count++;
							if (count % howOften == 0) {
								callback.callback(title, count);
							}
							if (gotValue) {
								page.setContent(value.toString());
							}
							// save(page);
							pages.add(page);
							if (pages.size() >= MAX_PAGES) {
								save(new ArrayList<Page>(pages));
								pages.clear();
							}
						}
						gotId = false;
						gotTitle = false;
						gotValue = false;
						gotRedirect = false;
						value = new StringBuilder();
						page = new Page();
						break;
					case "id":
						inId = false;
						break;
					case "ns":
						inNs = false;
						break;
					case "title":
						inTitle = false;
						break;
					case "text":
						inValue = false;
						break;
					case "redirect":
						inRedirect = false;
						break;
					default:
				}
			} else if (eventType == XMLStreamConstants.CHARACTERS) {
				char[] chars = null;
				if (inPage && (inId || inTitle || inValue || inRedirect)) {
					chars = new char[reader.getTextLength()];
					reader.getTextCharacters(0, chars, 0, chars.length);
					if (inPage && inId) {
						id = Integer.parseInt(new String(chars));
						page.setId(id);
						gotId = true;
					} else if (inPage && inTitle) {
						title = new String(chars);
						page.setTitle(title);
						gotTitle = true;
					} else if (inPage && inValue) {
						value.append(chars);
						gotValue = true;
					} else if (inPage && inNs) {
						int code = Integer.parseInt(new String(chars));
						page.setNs(PageNamespace.getFromCode(code));
					}
				}
			} else if (eventType == XMLStreamConstants.ATTRIBUTE) {
				if (inRedirect) {

				}
			}
		}
		if (!pages.isEmpty()) {
			save(pages);
		}
		close();
	}

	public void close() throws IOException, SQLException, InterruptedException {
		exec.shutdown();
		exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		try (Statement stmt = conn.createStatement()) {
			stmt.executeUpdate("create index title_index on wiki (title);");
			stmt.executeUpdate("vacuum");
		}
		for (FileChannel channel : channels) {
			channel.close();
		}
		conn.close();
		if (iwriter != null) {
			iwriter.close();
			directory.close();
		}
	}

	public void save(List<Page> pages) throws InterruptedException, IOException {

		exec.submit(new Runnable() {

			@Override
			public void run() {

				try (PreparedStatement stmt = conn.prepareStatement("insert into titles (_id, title) VALUES (?,?);");
						PreparedStatement dataStmt = conn
								.prepareStatement("insert into wiki (_id, title, redirect, file, position, length) VALUES (?,?,?,?,?,?);")) {
					for (Page page : pages) {
						try {
							if (page.getRedirect() == null && page.getContent() == null) {
								System.out.println("Null redirect and content: " + page.getTitle());
								continue;
							}
							byte[] bytes = null;
							WhereToWrite toWrite = null;
							if (page.getRedirect() == null) {
								String val = WikiModel.toHtml(page.getContent());
								ByteArrayOutputStream boas = new ByteArrayOutputStream();
								boas.write(page.getTitle().length());
								boas.write(page.getTitle().getBytes("UTF-8"));
								boas.write(val.getBytes("UTF-8"));
								if (compress) {
									bytes = compress(new ByteArrayInputStream(boas.toByteArray()));
								} else {
									bytes = boas.toByteArray();
								}
								toWrite = allocate(bytes.length);
								toWrite.channel.write(ByteBuffer.wrap(bytes), toWrite.position);
							}

							stmt.setInt(1, page.getId());
							stmt.setString(2, page.getTitle());
							stmt.addBatch();

							dataStmt.setInt(1, page.getId());
							dataStmt.setString(2, page.getTitle());
							if (page.getRedirect() == null) {
								dataStmt.setNull(3, Types.VARCHAR);
								dataStmt.setLong(4, toWrite.fileNumber);
								dataStmt.setLong(5, toWrite.position);
								dataStmt.setLong(6, bytes.length);
							} else {
								dataStmt.setString(3, page.getRedirect());
								dataStmt.setNull(4, Types.INTEGER);
								dataStmt.setNull(5, Types.INTEGER);
								dataStmt.setNull(6, Types.INTEGER);
							}
							dataStmt.addBatch();
						} catch (Exception ex) {
							System.err.println("Exception during: " + page.getTitle());
							ex.printStackTrace();
						}
					}
					stmt.executeBatch();
					dataStmt.executeBatch();
				} catch (SQLException ex) {
					ex.printStackTrace();
				}

			}
		});
		if (iwriter != null) {
			// exec.submit(new Runnable() {
			//
			// @Override
			// public void run() {
			// Document doc = new Document();
			// doc.add(new Field("title", page.getTitle(), TextField.TYPE_NOT_STORED));
			// doc.add(new Field("value", page.getContent(), TextField.TYPE_NOT_STORED));
			// doc.add(new LongField("id", page.getId(), Store.YES));
			// try {
			// iwriter.addDocument(doc);
			// } catch (IOException e) {
			// e.printStackTrace();
			// }
			// }
			// });

		}
	}

	public static byte[] compress(InputStream in) throws IOException {
		try (ByteArrayOutputStream boas = new ByteArrayOutputStream();
				GZIPOutputStream out = new GZIPOutputStream(boas)) {
			IOUtils.copy(in, out);
			out.close();
			return boas.toByteArray();
		}
	}

	public synchronized WhereToWrite allocate(int size) throws IOException {
		FileChannel channel = null;
		if (!channels.isEmpty()) {
			// Get the latest channel
			channel = channels.get(channels.size() - 1);
		}
		// Open initial channel or a new one is its greater than the max size
		if (channel == null || channel.size() + size >= maxFileSize) {
			channel = openFile(new File(outputDirectory, "dat." + ++fileNumber));
			channels.add(channel);
		}
		WhereToWrite toRet = new WhereToWrite();
		toRet.channel = channel;
		toRet.position = channel.position();
		toRet.fileNumber = fileNumber;
		channel.position(toRet.position + size);
		return toRet;
	}

	@SuppressWarnings("resource")
	public static FileChannel openFile(File file) throws IOException {
		return new RandomAccessFile(file, "rw").getChannel();
	}

	public static class WhereToWrite {

		FileChannel channel;
		long position;
		int fileNumber;
	}

	public static class LimitedQueue<E> extends LinkedBlockingQueue<E> {

		private static final long serialVersionUID = -4166638705262786442L;

		public LimitedQueue(int maxSize) {
			super(maxSize);
		}

		@Override
		public boolean offer(E e) {
			// turn offer() and add() into a blocking calls (unless interrupted)
			try {
				put(e);
				return true;
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
			}
			return false;
		}

	}
}
