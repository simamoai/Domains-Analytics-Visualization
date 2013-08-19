package edu.jhu.researchProject.mapReduceProcess;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import edu.jhu.researchProject.dao.AccessedWebPageDAO;
import edu.jhu.researchProject.dao.AccessedWebPageDAOImpl;
import edu.jhu.researchProject.model.AccessedWebPage;

public class DomainNamesReducer extends Reducer<Text, Text, Text, Text> {
	private static final String KEY_SPACE = "webpagekeyspace";
	private AccessedWebPageDAO accessedWebPageDAO = new AccessedWebPageDAOImpl("localhost:9160", KEY_SPACE);
	private AccessedWebPage reducedAccessedWebPage;
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String pageCode = "";
		String ipAddress = "";
		String content = "";

		for (Text value : values) {
			String[] valuesArr = value.toString().split("~");
			if (valuesArr.length == 3) {
				pageCode = valuesArr[0];
				ipAddress = valuesArr[1];
				content = valuesArr[2];
			}
		}
		this.reducedAccessedWebPage = new AccessedWebPage(key.toString(),
				content, ipAddress, pageCode);
		if (reducedAccessedWebPage != null) {
			accessedWebPageDAO.write(key.toString(), this.reducedAccessedWebPage.getEntryMap());
			context.write(key, new Text(reducedAccessedWebPage.toString()));
		}
	}
}
