package edu.jhu.researchProject.model;

import java.util.HashMap;
import java.util.Map;

public class AccessedWebPage {
	private String pageKey;
	private String pageContent;
	private String ipAddress;
	private String pageCode;
	
	public AccessedWebPage() {}
	
	public AccessedWebPage(String pageKey, String pageContent,
			String ipAddress, String pageCode) {
		this.pageKey = pageKey;
		this.pageContent = pageContent;
		this.ipAddress = ipAddress;
		this.pageCode = pageCode;
	}
	
	public AccessedWebPage(String pageKey, String pageContent) {
		super();
		this.pageKey = pageKey;
		this.pageContent = pageContent;
	}

	public synchronized String getPageContent() {
		return pageContent;
	}
	
	public synchronized void setPageContent(String pageContent) {
		this.pageContent = pageContent;
	}
	
	public synchronized String getPageKey() {
		return pageKey;
	}
	
	public synchronized void setPageKey(String pageKey) {
		this.pageKey = pageKey;
	}

	
	public synchronized String getIpAddress() {
		return ipAddress;
	}

	public synchronized void setIpAdress(String ipAddress) {
		this.ipAddress = ipAddress;
	}

	public synchronized String getPageCode() {
		return pageCode;
	}

	public synchronized void setPageCode(String pageCode) {
		this.pageCode = pageCode;
	}

	@Override
	public String toString() {
		return pageKey + " "+ pageContent + " " + ipAddress + " "+ pageCode;
	}

	@Override
	public boolean equals(Object obj) {
		AccessedWebPage accessedWebPage = null;
		if (obj instanceof AccessedWebPage) {
			accessedWebPage = (AccessedWebPage)obj;
		}
		
		return this.pageKey.equals(accessedWebPage.pageKey) && 
				this.pageContent.equals(accessedWebPage.pageContent);
	}

	public Map<String, String> getEntryMap() {
		Map<String,String> entryMap = new HashMap<String,String>();
		entryMap.put("pageKey", this.pageKey);
		entryMap.put("pageContent", this.pageContent);
		entryMap.put("ipAddress", this.ipAddress);
		entryMap.put("pageCode", this.pageCode);
		return entryMap;
	}
	
}
