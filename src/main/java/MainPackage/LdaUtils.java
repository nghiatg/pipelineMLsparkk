package MainPackage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.mllib.clustering.DistributedLDAModel;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class LdaUtils {
	
	static SparkConf conf = new SparkConf().setAppName("JavaKLatentDirichletAllocationExample").setMaster("local[4]");
	static JavaSparkContext jsc = new JavaSparkContext(conf);
	
	public static LdaVocab ldaTest(List<String> docs,int numberOfTopic,int ite) throws IOException {

		jsc.setCheckpointDir("C:\\Users\\Mr-Tuy\\Downloads\\bigdata\\sparkbin\\checkpoint");
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

		Dataset<Row> inputForCV = removeStopWords(docs);
//		System.out.println(inputForCV.first().toString());
//		inputForCV.show(false);

		CountVectorizerModel cvModel = new CountVectorizer().setInputCol("NoStopWords").setOutputCol("features")
				.setVocabSize(20000).setMinDF(1).fit(inputForCV);
		Dataset<Row> afterCV = cvModel.transform(inputForCV);
		String[] vocabulary = cvModel.vocabulary();

		Dataset<Row> wordCount = afterCV.select("features");
		List<Vector> lv = getVectorForLdaInput(wordCount, vocabulary.length);

		JavaRDD<Vector> parsedData = jsc.parallelize(lv);

		JavaPairRDD<Long, Vector> corpus = JavaPairRDD.fromJavaRDD(parsedData.zipWithIndex().map(Tuple2::swap));
		corpus.cache();
		
		DistributedLDAModel ldaModel = (DistributedLDAModel) (new LDA().setK(numberOfTopic).setMaxIterations(ite).setCheckpointInterval(10).run(corpus));

//		System.out.println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize() + " words):");
//		Matrix topics = ldaModel.topicsMatrix();
//		for (int topic = 0; topic < numberOfTopic; topic++) {
//			System.out.println("Topic " + topic + ":");
//			for (int word = 0; word < ldaModel.vocabSize(); word++) {
//				System.out.println(vocabulary[word] + ":\t" + topics.apply(word, topic));
//			}
//		}
//
//		jsc.close();
		return new LdaVocab(vocabulary, ldaModel);
	}
	
	public static Dataset<Row> removeStopWords(List<String> docs) throws IOException {
		SparkSession spark = SparkSession.builder().appName("JavaPipelineExample").master("local[4]").getOrCreate();
		StopWordsRemover remover = new StopWordsRemover().setInputCol("raw").setOutputCol("NoStopWords");
		remover.setStopWords(SupportClass.getStopWordsVN().toArray(new String[0]));

		ArrayList<Row> data = new ArrayList<Row>();
		int id = 0 ;
		for(String doc : docs) {
			data.add(changeStringToRow(doc,id));
			id++;
		}
		StructType schema = new StructType(new StructField[] {
				new StructField("id", DataTypes.LongType, false, Metadata.empty()),
				new StructField("raw", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty()) });

		Dataset<Row> dataset = spark.createDataFrame(data, schema);
		dataset.show();
		Dataset<Row> afterFiltered = remover.transform(dataset).select("NoStopWords");
		return afterFiltered;
	}
	
	public static Row changeStringToRow(String doc, int id) {
		int pos = 0, end = 0;
		List<String> words = new ArrayList<String>();
		while (end < doc.length()) {
			end = doc.indexOf(' ', pos);
			if(end >= doc.length() || end == -1) {
				break;
			}
			String word = doc.substring(pos, end);
			int bIdx = 0; 
			int eIdx = word.length()-1;
			if(eIdx > bIdx) {
				try {
					while(!Character.isLetterOrDigit(word.charAt(bIdx)) && bIdx <= eIdx) bIdx++;
					while(!Character.isLetterOrDigit(word.charAt(eIdx)) && bIdx <= eIdx) eIdx--;
					if(eIdx > bIdx) {
						words.add(word.substring(bIdx, eIdx+1).toLowerCase());
					}
				}catch(Exception e) {
					
				}
			}
			pos = end + 1;
		}
		Row r = RowFactory.create(new Long(id),words);
		return r;
	}

	public static List<Vector> getVectorForLdaInput(Dataset<Row> wordCount, int vocabularySize) {
		List<Vector> lv = new ArrayList<Vector>();
		for (Row r : wordCount.collectAsList()) {
			try {
				System.out.println(r.toString());
				String element2 = r.toString().split("\\[")[2];
				String indexContent = element2.substring(0, element2.length() - 2);
				String[] indexAsString = indexContent.split(",");
				int[] index = new int[indexAsString.length];
				for (int i = 0; i < indexAsString.length; ++i) {
	//				if(indexAsString[i].equals("")) System.out.println(r.toString());
					index[i] = Integer.parseInt(indexAsString[i]);
				}
	
				String element3 = r.toString().split("\\[")[3];
				String countContent = element3.substring(0, element3.length() - 3);
				String[] countAsString = countContent.split(",");
				double[] countNotOrder = new double[countAsString.length];
				for (int i = 0; i < countAsString.length; ++i) {
					countNotOrder[i] = Double.parseDouble(countAsString[i]);
				}
	
				double[] count = new double[vocabularySize];
				for (int i = 0; i < indexAsString.length; ++i) {
					count[index[i]] = countNotOrder[i];
				}
				lv.add(Vectors.dense(count));
			}catch(Exception e) {
				continue;
			}
		}
		return lv;
	}
	
	public static ArrayList<File> allDataFiles(String path,String tail) {
		ArrayList<File> result = new ArrayList<File>();
		File parentFile = new File(path);
		if(parentFile.getName().substring(parentFile.getName().length()-tail.length()).equals(tail)) {
			result.add(parentFile);
		}else if(parentFile.isDirectory()) {
			File[] subfiles = parentFile.listFiles(new FileFilter() {
	
				public boolean accept(File pathname) {
					if(!pathname.isHidden()) {
						return true;
					}
					return false;
				}
			});
			for(File f : subfiles) {
				if(f.getName().length() > 5) {
					if(f.getName().substring(f.getName().length()-tail.length()).equals(tail)) {
						result.add(f);
					}else {
						if(f.isDirectory()) {
							result.addAll(allDataFiles(f.getAbsolutePath(),tail));
						}
					}
				}
			}
		}
		return result;
	}
	
	public static List<String> listAllContents(String path) throws IOException {
		List<String> result = new ArrayList<String>();
		for(File f : allDataFiles(path,".txt")) {
			StringBuilder sb = new StringBuilder();
			BufferedReader br = new BufferedReader(new FileReader(f));
			String line = br.readLine();
			while(line!= null) {
				sb.append(line + " ");
				line = br.readLine();
			}
			result.add(sb.toString());
			br.close();
		}
		return result;
	}
	
	public static Vector testOneDoc(LdaVocab lv , String doc) {
		String[] vocab = lv.getVocab();
		String[] docElement = doc.split(" ");
		int start = 0 ;
		int end;
		String oneElement;
		for(int  i = 0 ; i < docElement.length ; ++i) {
			oneElement = docElement[i];
			end = oneElement.length() - 1;
			try {
				while(!Character.isLetterOrDigit(oneElement.charAt(start))) start++;
				while(!Character.isLetterOrDigit(oneElement.charAt(end))) end--;
				docElement[i] = oneElement.substring(start, end+1);
			}catch(Exception e) {
				
			}
		}
		HashMap<String,Integer> vocabulary = new HashMap<String,Integer>();
		for(int i =0 ; i < vocab.length; ++i) {
			vocabulary.put(vocab[i], i);
		}
		HashMap<Integer,Integer> wordCountBaseVocabulary = new HashMap<Integer,Integer>();
		for(String element : docElement) {
			if(vocabulary.containsKey(element)) {
				if(wordCountBaseVocabulary.containsKey(vocabulary.get(element))) {
					wordCountBaseVocabulary.put(vocabulary.get(element),wordCountBaseVocabulary.get(vocabulary.get(element)) + 1);
				}else {
					wordCountBaseVocabulary.put(vocabulary.get(element),1);
				}
			}
		}
		double[] wordCountAsArray = new double[vocabulary.keySet().size()];
		for(int i : wordCountBaseVocabulary.keySet()) {
			wordCountAsArray[i] = (double)(wordCountBaseVocabulary.get(i));
		}
		Vector newDocumentDistribution = lv.getModel().toLocal().topicDistribution(Vectors.dense(wordCountAsArray));
//		double sum = 0.0;
//		for(int d =0 ; d<newDocumentDistribution.size(); ++d) {
//			System.out.println(d + "\t" + newDocumentDistribution.apply(d));
//			sum += newDocumentDistribution.apply(d);
//		}
//		System.out.println(sum);
		return newDocumentDistribution;
	}
}