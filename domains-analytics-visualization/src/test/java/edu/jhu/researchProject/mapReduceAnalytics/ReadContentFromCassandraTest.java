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
		//File resultDistinctDir = new File("/home/training/git/Domains-Analytics-Visualization/domains-analytics-visualization/src/main/resources/data-read-from-cassandra");
		delete(resultTop10Dir);
	}
	@After
	public void runScriptInProcessUsingPig() {
		ProcessUsingPig.runPigScript();
	}
	
	@Test
	public void testReadContentFromCassandraMapReduceJob() throws Exception{
		new ReadContentFromCassandraWithAnalytics().run();
	}
	
	public void delete(File f) {
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
