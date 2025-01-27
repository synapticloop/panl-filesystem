package com.synapticloop;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;

import java.io.*;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Main {
	// these are the directories to ignore
	private static final Set<String> IGNORE_DIRECTORIES = new HashSet<>();

	static {
		IGNORE_DIRECTORIES.add("build");
		IGNORE_DIRECTORIES.add("gradle");
	}

	// This is the file filter that is used for both files and directories
	private static final IOFileFilter FILE_FILTER = new IOFileFilter() {
		@Override public boolean accept(File file) {
			return (accept(file, file.getName()));
		}

		@Override public boolean accept(File file, String fileName) {
			if (IGNORE_DIRECTORIES.contains(fileName)) {
				return (false);
			}

			return (!fileName.startsWith("."));
		}
	};

	public static void main(String[] args) {
		if (args.length == 0) {
			throw new RuntimeException(
					"Expecting one argument of the base directory to index.");
		}

		// try and find the passed in directory
		File baseDir = new File(args[0]);
		if (!baseDir.exists()) {
			throw new RuntimeException(
					"Base directory " +
							args[0] +
							" does not exist.");
		}

		// at this point we are good to index files

		for (File listFile : FileUtils.listFiles(
				baseDir,
				FILE_FILTER,
				FILE_FILTER
		)) {
			indexDocument(baseDir, listFile);
		}
	}

	/**
	 * <p>This does the heavy lifting of indexing the documents with Apache Tika,
	 * then connecting to the Solr server to add the contents of the document and metadata to the search collection
	 * index.</p>
	 *
	 * @param baseDir The base directory for starting the search indexing
	 * @param listFile The file to be indexed
	 */
	private static void indexDocument(File baseDir, File listFile) {
		// get the SolrJ client connection to Solr
		CloudSolrClient client = new CloudHttp2SolrClient.Builder(
				List.of("http://localhost:8983/solr/")).build();

		// Extract the information that we will be using for indexing
		String absolutePath = listFile.getAbsolutePath();
		String filePath =
				absolutePath.substring(
						baseDir.getAbsolutePath().length(),
						absolutePath.lastIndexOf(File.separator) + 1);
		String fileName = listFile.getName();

		String id = filePath + fileName;
		String fileType = fileName.substring(fileName.lastIndexOf(".") + 1);

		// the following is done because the Windows file separator is a backslash
		// '\' which interferes with the regex parsing on Windows file systems
		// only.
		String[] categories = filePath.split(
				(File.separator.equals("\\") ? "\\\\" : File.separator)
		);

		// a nicety to put them in the root directory
		if (categories.length == 0) {
			categories = new String[]{"ROOT_DIRECTORY"};
		}

		try {
			// get the contents automatically with the Tika parsing
			String contents = new Tika().parseToString(listFile);

			// create the solr document that is going to be indexed
			SolrInputDocument doc = new SolrInputDocument();

			// Add the fields to the document, the first parameter of the call is the
			// Solr field name - which must match the schema
			doc.addField("id", id);
			doc.addField("filename", fileName);
			doc.addField("filetype", fileType);
			doc.addField("contents", contents);
			doc.addField("category", categories);

			// now we add the document to the collection "filesystem", which must
			// match the collection that was defined in Solr
			client.add("filesystem", doc);

			// now commit the changes to the filesystem Solr schema
			client.commit("filesystem");

			System.out.println("Indexed file " + listFile.getAbsolutePath());
		} catch (IOException | TikaException | SolrServerException e) {
			// something went wrong - we will ignore this
			System.out.println(
					"Could not index file " +
							listFile.getAbsolutePath() +
							", message was: " +
							e.getMessage());
		}

		try {
			// don't forget to close the client
			client.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}