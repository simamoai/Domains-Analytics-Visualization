package edu.jhu.researchProject.dao;
import java.util.Map;

import com.netflix.astyanax.model.ColumnList;

public interface AccessedWebPageDAO {
	public void write(String rowKey, Map<String, String> columns);
	public ColumnList<String> read(String rowKey);
	public void delete(String rowKey);
}
