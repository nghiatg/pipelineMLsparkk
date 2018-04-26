package MainPackage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class MainClass {

	public static void main(String[] args) throws IOException {
		
//		TfidfByCV tiCV = new TfidfByCV(path ,null);
//		int totalCorpus = tiCV.getVectors().size();
//		ArrayList<Integer> idx = new ArrayList<Integer>(totalCorpus);
//		for(int index = 0 ; index < totalCorpus ; ++index) {
//			idx.add(index);
//		}
//		OneCorpus oc = new OneCorpus(tiCV.getVectors(),totalCorpus,idx);
		
		
//		String path2 = "D:\\textfiles\\lda-sample\\sample.txt";
//		TfidfPipelineExample.runTFIDF(path);
//		LdaPipeline.testSWR(path);

		
//		int[] index = new int[100];
//		for(int i = 0 ; i< 100 ; ++i) {
//			if(i<50) {
//				index[i] = i;
//			}else {
//				index[i] = i+55;
//			}
//		}
//		LdaVocab lv = LdaPipeline.ldaTest(path,index);
//		LdaPipeline.testNewDoc("D:\\textfiles\\bbcsport\\athletics&cricket\\001.txt", lv);
//		LdaPipeline.testNewDoc("D:\\textfiles\\bbcsport\\athletics&cricket\\100.txt", lv);
//		LdaPipeline.testNewDoc("D:\\textfiles\\bbcsport\\athletics&cricket\\053.txt", lv);
//		LdaPipeline.testNewDoc("D:\\textfiles\\bbcsport\\athletics&cricket\\072.txt", lv);
//		LdaPipeline.testNewDoc("D:\\textfiles\\bbcsport\\athletics&cricket\\018.txt", lv);
//		LdaPipeline.testNewDoc("D:\\textfiles\\bbcsport\\athletics&cricket\\020.txt", lv);
//		System.out.println("\n\npart 2 : ");
//		LdaPipeline.testNewDoc("D:\\textfiles\\bbcsport\\athletics&cricket\\300.txt", lv);
//		LdaPipeline.testNewDoc("D:\\textfiles\\bbcsport\\athletics&cricket\\301.txt", lv);
//		LdaPipeline.testNewDoc("D:\\textfiles\\bbcsport\\athletics&cricket\\302.txt", lv);
//		LdaPipeline.testNewDoc("D:\\textfiles\\bbcsport\\athletics&cricket\\303.txt", lv);
//		LdaPipeline.testNewDoc("D:\\textfiles\\bbcsport\\athletics&cricket\\304.txt", lv);
//		
//		long end = System.nanoTime();
//		System.out.println((end - start)/1000000);
		
		
//		HashMap<Long,Integer> gender = URL.createGenderSample("D:\\textfiles\\db\\asg.dat");
//		HashMap<String,Double> urlOneSide = URL.urlOneSide("D:\\textfiles\\db",gender ,true);
//		for(String key : urlOneSide.keySet()) {
//			if(key.length() > 10) {
//				System.out.println(key.substring(0,10) + "  :  " + urlOneSide.get(key));
//			}else {
//				System.out.println(key + "  :  " + urlOneSide.get(key));
//			}
//		}
		
		
		
		
		
		
		
		
		
//		List<String> docs = LdaUtils.listAllContents(path);
//		LdaVocab lv = LdaUtils.ldaTest(docs);
//
//		ArrayList<Vector> firstField = new ArrayList<Vector>();
//		ArrayList<Vector> secondField = new ArrayList<Vector>();
//		ArrayList<File> allSubFiles = LdaUtils.allDataFiles(path, ".txt");
//		for(int i=0 ; i < allSubFiles.size()-1 ; ++i) {
//			File f = allSubFiles.get(i);
//			if(i < 124) {
//				firstField.add(LdaUtils.testOneDoc(lv, LdaUtils.listAllContents(f.getAbsolutePath()).get(0)));
//			}else {
//				secondField.add(LdaUtils.testOneDoc(lv, LdaUtils.listAllContents(f.getAbsolutePath()).get(0)));
//			}
//		}
//		
//		
//		double[] averageFirst = new double[firstField.get(0).size()];
//		for(Vector v : firstField) {
//			double[] doubleV = v.toArray();
//			for(int i = 0 ; i < averageFirst.length ; ++i) {
//				averageFirst[i] += doubleV[i];
//			}
//		}
//		for(int i = 0 ; i < averageFirst.length ; ++i) {
//			averageFirst[i] /= (double)firstField.size();
//		}
//		SupportClass.printarray(averageFirst);
//		System.out.println("\n\n");
//		
//		double[] averageSecond = new double[secondField.get(0).size()];
//		for(Vector v : secondField) {
//			double[] doubleV = v.toArray();
//			for(int i = 0 ; i < averageSecond.length ; ++i) {
//				averageSecond[i] += doubleV[i];
//			}
//		}
//		for(int i = 0 ; i < averageSecond.length ; ++i) {
//			averageSecond[i] /= (double)secondField.size();
//		}
//		SupportClass.printarray(averageSecond);
//		
//		
//		
//		long end = System.nanoTime();
//		System.out.println((end - start)/1000000);
		
		
		
		
		
		
		
//		PrintWriter pr = new PrintWriter("C:\\Users\\VCCORP\\Desktop\\label\\bias\\svm_data");
//		ArrayList<String> docs = SupportClass.loadContent(new String[] {"C:\\Users\\VCCORP\\Desktop\\label\\bias\\url0","C:\\Users\\VCCORP\\Desktop\\label\\bias\\url1"});
//		LdaVocab lv = LdaUtils.ldaTest(docs);

//		ArrayList<Vector> firstField = new ArrayList<Vector>();
//		ArrayList<Vector> secondField = new ArrayList<Vector>();
//		for(int i=0 ; i < docs.size() ; ++i) {
//			String doc = docs.get(i);
//			if(i < 138) {
//				firstField.add(LdaUtils.testOneDoc(lv,doc));
//			}else {
//				secondField.add(LdaUtils.testOneDoc(lv, doc));
//			}
//		}
//		
//		
//		double[] averageFirst = new double[firstField.get(0).size()];
//		for(Vector v : firstField) {
//			double[] doubleV = v.toArray();
//			for(int i = 0 ; i < averageFirst.length ; ++i) {
//				averageFirst[i] += doubleV[i];
//			}
//		}
//		for(int i = 0 ; i < averageFirst.length ; ++i) {
//			averageFirst[i] /= (double)firstField.size();
//		}
//		SupportClass.printarray(averageFirst);
//		System.out.println("\n\n");
//		
//		double[] averageSecond = new double[secondField.get(0).size()];
//		for(Vector v : secondField) {
//			double[] doubleV = v.toArray();
//			for(int i = 0 ; i < averageSecond.length ; ++i) {
//				averageSecond[i] += doubleV[i];
//			}
//		}
//		for(int i = 0 ; i < averageSecond.length ; ++i) {
//			averageSecond[i] /= (double)secondField.size();
//		}
//		SupportClass.printarray(averageSecond);
		
		
		
		run1();
//		System.out.println("\n\n\n1 loop\n\n\n");
//		run2();
		
	}
	
	public static void run1() throws IOException {
		ArrayList<String> docs = SupportClass.loadContent(new String[] {"E:\\study\\text\\bias\\moreURL","E:\\study\\text\\bias\\filteredURL3"});
		LdaVocab lv = LdaUtils.ldaTest(docs,15,1200);
		double[] v;
		StringBuilder sb = new StringBuilder();
		int i = 0 ;
		for(int index = 0 ; index < docs.size() ; ++index) {
			String doc = docs.get(index);
			if(index < 263) {
				sb.append(0);
				v = LdaUtils.testOneDoc(lv, doc).toArray();
				while( i < v.length) {
					sb.append(" ").append(i+1).append(":").append(v[i]);
					++i;
				}
				System.out.println(sb.toString());
			}else {
				sb.append(1);
				v = LdaUtils.testOneDoc(lv, doc).toArray();
				while( i < v.length) {
					sb.append(" ").append(i+1).append(":").append(v[i]);
					++i;
				}
				System.out.println(sb.toString());
			}
			sb.setLength(0);
			i = 0;
		}
		
		
//		ArrayList<String> docs = SupportClass.loadContent(new String[] {"C:\\Users\\VCCORP\\Desktop\\label\\bias\\filteredURL"});
//		LdaVocab lv = LdaUtils.ldaTest(docs);
		System.out.println("\n\n\ndone training\n\n\n");
//		HashMap<String,ArrayList<String>> gc = SupportClass.guidContent();
		SupportClass.test2(lv);
	}
	
	public static void run2() throws IOException{
		ArrayList<String> docs = SupportClass.loadContent(new String[] {"E:\\study\\text\\bias\\moreURL","E:\\study\\text\\bias\\filteredURL3"});
		LdaVocab lv = LdaUtils.ldaTest(docs,12,1200);
		double[] v;
		StringBuilder sb = new StringBuilder();
		int i = 0 ;
		for(int index = 0 ; index < docs.size() ; ++index) {
			String doc = docs.get(index);
			if(index < 263) {
				sb.append(0);
				v = LdaUtils.testOneDoc(lv, doc).toArray();
				while( i < v.length) {
					sb.append(" ").append(i+1).append(":").append(v[i]);
					++i;
				}
				System.out.println(sb.toString());
			}else {
				sb.append(1);
				v = LdaUtils.testOneDoc(lv, doc).toArray();
				while( i < v.length) {
					sb.append(" ").append(i+1).append(":").append(v[i]);
					++i;
				}
				System.out.println(sb.toString());
			}
			sb.setLength(0);
			i = 0;
		}
		
		
		
		
		
		
		
		
//		ArrayList<String> docs = SupportClass.loadContent(new String[] {"C:\\Users\\VCCORP\\Desktop\\label\\bias\\filteredURL"});
//		LdaVocab lv = LdaUtils.ldaTest(docs);
		System.out.println("\n\n\ndone training\n\n\n");
//		HashMap<String,ArrayList<String>> gc = SupportClass.guidContent();
		SupportClass.test2(lv);
	}

}