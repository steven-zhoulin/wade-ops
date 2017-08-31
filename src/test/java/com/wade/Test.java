package com.wade;

import org.apache.commons.io.DirectoryWalker;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class Test {

	
	public static void main(String[] args) throws Exception {
		
		ListFileWorker worker = new ListFileWorker();
		
		List<File> files = worker.list(new File("D:\\eclipse-workspace\\hello\\bomc"));
		for (File file : files) {
			System.out.println(file);
			FileInputStream fis = new FileInputStream(file);
			ObjectInputStream ois = new ObjectInputStream(fis);
			
			try {
				
				while (true) {
					
					Map map = (Map) ois.readObject();
					String probetype = (String) map.get("probetype");
					if ("browser".equals(probetype)) {
						System.out.println(map);
					}
				}
							
			} catch (EOFException ee) {
				System.out.println("�ļ������������!");
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				ois.close();
			}
		}

		
	}
		
	private static class ListFileWorker extends DirectoryWalker<File> {
		private List<File> list(File directory) throws IOException {
			List<File> files = new ArrayList<File>();
			walk(directory, files);
			return files;
		}

		@Override
		protected void handleFile(File file, int depth, Collection<File> results) throws IOException {
			String filename = file.getName();
			if (!filename.startsWith("bomc.web")) {
				return;
			}
			
			if (filename.endsWith("11111240.dat")) {
				results.add(file);
			}
		}
	}

}
