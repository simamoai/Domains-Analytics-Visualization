package edu.jhu.researchProject.mapReduceAnalytics;

import java.io.File;
import java.io.FileNotFoundException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.jhu.researchProject.mapReduceAnalytics.ReadContentFromCassandraWithAnalytics;
import edu.jhu.researchProject.visualDataHelper.ProcessUsingPig;

public class ReadContentFromCassandraTest {
	@Before
	public void removeGeneratedFiles() {
		File resultTop10Dir = new File("/home/training/Desktop/result_top10/");
		String davRoot = "/home/training/git/Domains-Analytics-Visualization/domains-analytics-visualization/";
		File dataReadFromCassandraDir = new File(davRoot
				+ "src/main/resources/data-read-from-cassandra");
		File resultDistinctDir = new File(davRoot + "result_distinct.out");
		File resultFilterDir = new File(davRoot + "result_filter");
		File resultOrderedDir = new File(davRoot + "result_ordered.out");
		delete(dataReadFromCassandraDir);
		delete(resultTop10Dir);
		delete(resultDistinctDir);
		delete(resultFilterDir);
		delete(resultOrderedDir);
	}

	@After
	public void runScriptInProcessUsingPig() {
		ProcessUsingPig.runPigScript();
	}

	@Test
	public void testReadContentFromCassandraMapReduceJob() throws Exception {
		new ReadContentFromCassandraWithAnalytics().run();
	}

	private void delete(File f) {
		if (f.isDirectory()) {
			for (File c : f.listFiles())
				delete(c);
		}
		if (!f.delete())
			try {
				throw new FileNotFoundException("Failed to delete file: " + f);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
	}
}
