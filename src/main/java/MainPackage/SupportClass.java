package MainPackage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class SupportClass {
	public static double getDistance(double[] firstVector, double[] secondVector) {
		double firstVectorValue = 0.0;
		double secondVectorValue = 0.0;
		double vectorMultiplication = 0.0;
		int size = firstVector.length;
		int index = 0;
		while(index < size) {
			firstVectorValue += firstVector[index] * firstVector[index];
			secondVectorValue += secondVector[index] * secondVector[index];
			vectorMultiplication += firstVector[index] * secondVector[index];
			index++;
		}
		double result = vectorMultiplication / (Math.sqrt(firstVectorValue * secondVectorValue));
		return (double)1 - result;
	}
	
	public static double[] getAverageVector(ArrayList<double[]> vectorsList) {
		int vectorSize = vectorsList.get(0).length;
		int listSize = vectorsList.size();
		double[] result = new double[vectorSize];
		int index = 0;
		while(index < vectorSize) {
			double sum = 0.0;
			for(int vectorCount = 0 ; vectorCount < listSize ; ++vectorCount) {
				sum += (vectorsList.get(vectorCount))[index];
			}
			result[index] = sum / ((double)listSize);
			index++;
		}
		
		return result;
	}
	

	public static void printlist(ArrayList<Integer> list) {
		StringBuilder sb = new StringBuilder();
		for(int e : list) {
			sb.append(e).append("\t");
		}
		System.out.println(sb.toString());
	}
	
	public static void printarray(double[] arr) {
		String s = "";
		for(Double d : arr) {
			s+= d.toString() + "\t";
		}
		System.out.println(s);
	}
	
	public static void printarray(int[] arr) {
		String s = "";
		for(Integer d : arr) {
			s+= d.toString() + "  ";
		}
		System.out.println(s);
	}
	
	public static boolean arrayContainValue(int[] arr, int value) {
		for(int i : arr) {
			if(value == i) {
				return true;
			}
		}
		return false;
	}
	
	public static Vector loadNewDocumentAsLdaInput(String path,String[] vocab) throws IOException {
		HashMap<String,Integer> vocabulary = new HashMap<String,Integer>();
		for(int i =0 ; i < vocab.length; ++i) {
			vocabulary.put(vocab[i], i);
		}
		BufferedReader br = new BufferedReader(new FileReader(path));
		HashMap<Integer,Integer> wordCountBaseVocabulary = new HashMap<Integer,Integer>();
		String line = br.readLine();
		while(line != null) {
			String[] lineElement = line.split(" ");
			for(String element : lineElement) {
				if(vocabulary.containsKey(element)) {
					if(wordCountBaseVocabulary.containsKey(vocabulary.get(element))) {
						wordCountBaseVocabulary.put(vocabulary.get(element),wordCountBaseVocabulary.get(vocabulary.get(element)) + 1);
					}else {
						wordCountBaseVocabulary.put(vocabulary.get(element),1);
					}
				}
			}
			line = br.readLine();
		}
		
		double[] wordCountAsArray = new double[vocabulary.keySet().size()];
		for(int i : wordCountBaseVocabulary.keySet()) {
			wordCountAsArray[i] = (double)(wordCountBaseVocabulary.get(i));
		}
		br.close();
		return Vectors.dense(wordCountAsArray);
	}
	
	public static HashMap<String,Double> findMaxValues(HashMap<String,Double> initial,int needs) {
		HashMap<String,Double> result = new HashMap<String,Double>();
		ArrayList<Double> values = new ArrayList<Double>();
		for(String k : initial.keySet()) {
			values.add(initial.get(k));
		}
		Collections.sort(values);
		int size = values.size();
		int index = size-1;
		boolean flag = true;
		while(flag) {
			for(String k : initial.keySet()) {
				if(initial.get(k) == values.get(index)) {
					result.put(k,values.get(index));
					if(result.size() == needs) {
						flag = false;
						break;
					}
				}
			}
			index--;
		}
		
		return result;
	}
	
	public static ArrayList<JavaDocument> getTrainingFilesFromDirectory(String path) throws IOException{
		ArrayList<JavaDocument> documentList = new ArrayList<JavaDocument>();
		File file = new File(path);
		File[] subfile = file.listFiles(new FileFilter() {

			public boolean accept(File pathname) {
				if(Integer.parseInt(pathname.getName().substring(0,3)) < 50 && !pathname.isHidden()) {
					return true;
				}
				return false;
			}
			
		});
		long id = 0;
		for(File f : subfile) {
			documentList.add(new JavaDocument(id,f));
			id++;
		}
		return documentList;
	}
	
	public static ArrayList<JavaDocument> getTestFilesFromDirectory(String path) throws IOException{
		ArrayList<JavaDocument> documentList = new ArrayList<JavaDocument>();
		File file = new File(path);
		File[] subfile = file.listFiles(new FileFilter() {

			public boolean accept(File pathname) {
				if(Integer.parseInt(pathname.getName().substring(0,3)) > 50 && !pathname.isHidden()) {
					return true;
				}
				return false;
			}
			
		});
		long id = 0;
		for(File f : subfile) {
			documentList.add(new JavaDocument(id,f));
			id++;
		}
		return documentList;
		
	}

	public static ArrayList<Row> getTrainingAsRowFilter(String path, int[] fileIndex) throws IOException {
		ArrayList<Row> result = new ArrayList<Row>();
		File file = new File(path);
		File[] subfile = file.listFiles(new FileFilter() {

			public boolean accept(File pathname) {
				if(!pathname.isHidden()) {
					return true;
				}
				return false;
			}
			
		});
//		for(int i = 0 ; i < subfile.length ; ++i) {
//			System.out.println(subfile[i]);
//		}
		long id = 0;
		if(fileIndex != null) {
			for(int i = 0; i < subfile.length ; ++i) {
				if(arrayContainValue(fileIndex,i)) {
					File f = subfile[i];
					ArrayList<String> wordsList = new ArrayList<String>();
					BufferedReader br = new BufferedReader(new FileReader(f.getAbsolutePath()));
					String line = br.readLine();
					while(line != null) {
						for (String word : line.split(" ")) {
							int begin = 0;
							while(begin < word.length() && !Character.isLetterOrDigit(word.charAt(begin))) {
								begin++;
							}
							int end = word.length()-1;
							while(end > 0 && !Character.isLetterOrDigit(word.charAt(end))) {
								end--;
							}
							if(end > begin)
							wordsList.add(word.substring(begin, end+1));
						}
						line = br.readLine();
					}
					result.add(RowFactory.create(new Long(id),wordsList));
					id++;
					br.close();
				}
			}
		}else {
			for(File f : subfile) {
				ArrayList<String> wordsList = new ArrayList<String>();
				BufferedReader br = new BufferedReader(new FileReader(f.getAbsolutePath()));
				String line = br.readLine();
				while(line != null) {
					for (String word : line.split(" ")) {
						int begin = 0;
						while(begin < word.length() && !Character.isLetterOrDigit(word.charAt(begin))) {
							begin++;
						}
						int end = word.length()-1;
						while(end > 0 && !Character.isLetterOrDigit(word.charAt(end))) {
							end--;
						}
						if(end > begin)
						wordsList.add(word.substring(begin, end+1));
					}
					line = br.readLine();
				}
				result.add(RowFactory.create(new Long(id),wordsList));
				id++;
				br.close();
			}
		}
		return result;
	}
	
	public static ArrayList<Row> getTrainingAsRow(String path) throws IOException{
		ArrayList<Row> result = new ArrayList<Row>();
		File file = new File(path);
		File[] subfile = file.listFiles(new FileFilter() {

			public boolean accept(File pathname) {
				if(!pathname.isHidden()) {
					return true;
				}
				return false;
			}
			
		});
		long id = 0;
		for(File f : subfile) {
			ArrayList<String> wordsList = new ArrayList<String>();
			BufferedReader br = new BufferedReader(new FileReader(f.getAbsolutePath()));
			String line = br.readLine();
			while(line != null) {
				for (String word : line.split(" ")) {
					wordsList.add(word);
				}
				line = br.readLine();
			}
			result.add(RowFactory.create(new Long(id),wordsList));
			id++;
			br.close();
		}
		return result;
	}
	
	public static double[] normalize(double[] vector) {
		int vectorLength = vector.length;
		double[] result = new double[vectorLength];
		double squareSum = 0.0;
		int index = 0; 
		while(index < vectorLength) {
			squareSum += vector[index] * vector[index];
			index++;
		}
		double rootSum = Math.sqrt(squareSum);
		index = 0;
		while(index < vectorLength) {
			result[index] = vector[index] / rootSum;
			index++;
		}
		return result;
		
	}
	
	public static double[] getCentroidAtUnitLength(ArrayList<double[]> vectors) {
		int vectorsSize = vectors.size();
		ArrayList<double[]> afterNormalized = new ArrayList<double[]>(vectorsSize);
		for(int vectorCount = 0; vectorCount < vectorsSize ; vectorCount++) {
			afterNormalized.add(normalize(vectors.get(vectorCount)));
		}
		double[] result = getAverageVector(afterNormalized);
		return result;
	}
	
	public static ArrayList<String> loadContent(String[] paths) throws IOException{
		ArrayList<String> result = new ArrayList<String>();
		for(String path : paths) {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
			int tab;
			String line = br.readLine();
			while(line != null) {
				try {
					tab = line.indexOf('\t');
					String contentFiltered = line.substring(tab+1).replaceAll(",", " ").replaceAll("\"", " ").replaceAll("-", " ").replaceAll("\\.", " ")
							.replaceAll("\\?", " ").replaceAll("\\(", " ").replaceAll("\\)", " ").replaceAll(":", " ").replaceAll("\\|", " ");
					if(contentFiltered.length() != 0) {
						result.add(contentFiltered);
					}
					line = br.readLine();
				}catch(Exception e) {
					System.out.println(e.toString());
				}
			}
			br.close();
		}
		return result;
	}
	
	public static ArrayList<String> getStopWordsVN() throws IOException{
		ArrayList<String> sw = new ArrayList<String>();
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("E:\\study\\text\\label\\stopwords.txt"), "UTF-8"));
		String line = br.readLine();
		while(line != null) {
			sw.add(line);
			line = br.readLine();
		}
		br.close();
		return sw;
	}
	
	public static HashMap<String,ArrayList<String>> guidContent() throws IOException{
		HashMap<String,ArrayList<String>> uc = new HashMap<String,ArrayList<String>>();
		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\VCCORP\\Desktop\\label\\gdata"));
		String line = br.readLine();
		int tab ;
		while(line != null) {
			tab = line.indexOf('\t');
			try {
				if(uc.containsKey(line.substring(0, tab))) {
					uc.get(line.substring(0, tab)).add(line.split("\t")[2]);
				}else {
					uc.put(line.substring(0, tab), new ArrayList<String>());
					uc.get(line.substring(0, tab)).add(line.split("\t")[2]);
				}
			}catch(Exception e) {
				System.out.println("error\t" +line + "\t|||\t" + line.split("\t").length);
			}
			
			line = br.readLine();
		}
		br.close();
//		for(String guid :uc.keySet()) {
//			System.out.println(guid + "\t" + uc.get(guid).get(0));
//		}
		return uc;
	}
	
	public static void test(LdaVocab lv , HashMap<String,ArrayList<String>> uc) {
		System.out.println(uc.size());
		ArrayList<double[]> distributions = new ArrayList<double[]>();
		HashMap<String,double[]> guidDistribution = new HashMap<String,double[]>();
		StringBuilder sb = new StringBuilder();
		int i,j;
		double sum = 0.0 ;
		for(String guid : uc.keySet()) {
			double[] average = new double[35];
			System.out.println(guid);
			try {
				for(String doc : uc.get(guid)) {
					distributions.add(LdaUtils.testOneDoc(lv, doc).toArray());
				}
				for(i = 0; i < distributions.get(0).length ; ++i ) {
					sum = 0.0;
					for(j = 0 ; j < distributions.size() ; ++j) {
						sum += distributions.get(j)[i];
					}
					average[i] = sum / (double)(distributions.size());
				}
				guidDistribution.put(guid, average);
				distributions.clear();
				sum = 0.0;
			}catch(Exception e) {
				
			}
		}
		System.out.println(guidDistribution.size());
		for(String guid : guidDistribution.keySet()) {
			sb.append(guid);
			for(i = 0 ; i < 35 ; ++i) {
				sb.append("\t").append(i+1).append(":").append(guidDistribution.get(guid)[i]);
			}
			System.out.println(sb.toString());
			sb.setLength(0);
		}
	}
	
	public static void test2(LdaVocab lv) throws IOException{
		BufferedReader br = new BufferedReader(new FileReader("E:\\study\\text\\bias\\gdata"));
		String line = br.readLine();
		StringBuilder sb = new StringBuilder();
		int count = 1;
		while(line != null) {
			try {
				double[] distribution = LdaUtils.testOneDoc(lv, line.split("\t")[2]).toArray();
				sb.append(count).append(" ").append(line.split("\t")[0]).append(" ").append(line.split("\t")[1]);
				for(int i = 0 ; i < distribution.length ; ++i) {
					sb.append(" ").append(i+1).append(":").append(distribution[i]);
				}
				System.out.println(sb.toString());
			}catch(Exception e) {
				
			}
			count++;
			line = br.readLine();
			sb.setLength(0);
		}
		br.close();
	}
}
