package edu.jhu.researchProject.mapReduceProcess;

import java.io.File;
import java.io.FileNotFoundException;

import org.junit.Before;
import org.junit.Test;

import edu.jhu.researchProject.mapReduceProcess.ExtractFromCompressedData;

public class ExtractFromCompressedDataTest {
	@Before
	public void removeGeneratedFiles() {
		String davRoot = "/home/training/git/Domains-Analytics-Visualization/domains-analytics-visualization/";
		File outputDir = new File(davRoot+"output");
		delete(outputDir);
	}
	@Test
	public void testExtractCompressedData() throws Exception{
		new ExtractFromCompressedData().run();
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
