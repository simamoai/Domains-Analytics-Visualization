package edu.jhu.researchProject.visualDataHelper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.pig.PigServer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class ProcessUsingPig {

	public static void runPigScript() {
		try {
			PigServer pigServer = new PigServer("local");
			runPigQuery(pigServer, "src/main/resources/data-read-from-cassandra/part-r-00000");
			generateChartDataJSONFile("lineChartDataResults.json", "line");
			generateChartDataJSONFile("barChartDataResults.json", "bar");
		} catch (Exception e) {
		}
	}
	public static void main(String[] args) {
		runPigScript();
	}

	public static void runPigQuery(PigServer pigServer, String inputFile)
			throws IOException {
		pigServer.registerQuery("A = load '" + inputFile
				+ "'as (x:chararray, y :int);");
		pigServer.registerQuery("B = order A by y desc;");
		pigServer.registerQuery("G = LIMIT B 10;");
		pigServer.registerQuery("C = distinct B;");
		pigServer.registerQuery("D = filter B by y > 50;");
		pigServer.store("B", "result_ordered.out");
		pigServer.store("C", "result_distinct.out");
		pigServer.store("D", "result_filter");
		pigServer.store("G", "/home/training/Desktop/result_top10");

	}
	
	@SuppressWarnings("unchecked")
	public static void generateChartDataJSONFile(String jsonFileName, String typeChart){
		JSONObject obj; // = new JSONObject();
		BufferedReader br = null;
		FileWriter writer = null;

		try {
			File file = new File("/home/training/Desktop/result_top10/part-r-00000");
			
			br = new BufferedReader(new FileReader(file));
			writer = createFileWriter("src/main/webapp/resources/jscharts-data/"+jsonFileName);
			String line;
			JSONObject jsCharts = new JSONObject();
			JSONObject jsChartsObj = new JSONObject();
			JSONObject dataTypeObj = new JSONObject();
			JSONArray datasetsList = new JSONArray();
			JSONArray optionsetList = new JSONArray();
			JSONObject optionsObj = new JSONObject();
			JSONArray dataList = new JSONArray();
			
			while ((line = br.readLine()) != null) {
				String[] arrayLine = line.split("\t");
				obj = new JSONObject();
				obj.put("unit", arrayLine[0]);
				obj.put("value", arrayLine[1]);
				dataList.add(obj);
			}
			dataTypeObj.put("type", typeChart);
			dataTypeObj.put("data", dataList);
			datasetsList.add(dataTypeObj);
			
			optionsObj.put("set", "setBackgroundColor");
			optionsObj.put("value", "'#efe'");
			optionsetList.add(optionsObj);
			optionsObj = new JSONObject();
			optionsObj.put("set", "setAxisNameX");
			optionsObj.put("value", "'Top Words'");
			optionsetList.add(optionsObj);
			optionsObj = new JSONObject();
			optionsObj.put("set", "setAxisNameY");
			optionsObj.put("value", "'Frequency'");
			optionsetList.add(optionsObj);
			optionsObj = new JSONObject();
			optionsObj.put("set", "setSize");
			optionsObj.put("value", "500,400");
			optionsetList.add(optionsObj);
			optionsObj = new JSONObject();
			optionsObj.put("set", "setTitle");
			optionsObj.put("value", "'Top Words in All Web Sites'");
			optionsetList.add(optionsObj);
			optionsObj = new JSONObject();
			optionsObj.put("set", "setTitleColor");
			optionsObj.put("value", "'#5555AA'");
			optionsetList.add(optionsObj);
			optionsObj = new JSONObject();
			optionsObj.put("set", "setTitleFontSize");
			optionsObj.put("value", "10");
			optionsetList.add(optionsObj);
			
			jsChartsObj.put("datasets", datasetsList);
			jsChartsObj.put("optionset", optionsetList);
			jsCharts.put("JSChart", jsChartsObj);
			System.out.println(jsCharts.toJSONString());
			writer.write(jsCharts.toJSONString()); 
		    writer.flush();
			br.close();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}
	public static FileWriter createFileWriter(String fileName) throws IOException {
		File file = new File(fileName);
		try {
			file.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
	     return new FileWriter(file);
	}

}
