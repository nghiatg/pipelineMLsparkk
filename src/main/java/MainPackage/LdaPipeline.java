package MainPackage;

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class LdaPipeline {
	public static LdaVocab ldaTest(String path,int[] fileIndex) throws IOException {
		
		SparkConf conf = new SparkConf().setAppName("JavaKLatentDirichletAllocationExample").setMaster("local[2]");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		
		Dataset<Row> inputForCV = removeStopWords(path,fileIndex);
		
		CountVectorizerModel cvModel = new CountVectorizer().setInputCol("NoStopWords").setOutputCol("features")
				.setVocabSize(20000)
				.setMinDF(2).fit(inputForCV);
		Dataset<Row> afterCV = cvModel.transform(inputForCV);
		String[] vocabulary = cvModel.vocabulary();
		
		Dataset<Row> wordCount = afterCV.select("features");
		List<Vector> lv = getVectorForLdaInput(wordCount,vocabulary.length);
		
		JavaRDD<Vector> parsedData = jsc.parallelize(lv);
		
		JavaPairRDD<Long, Vector> corpus = JavaPairRDD.fromJavaRDD(parsedData.zipWithIndex().map(Tuple2::swap));
		corpus.cache();

		DistributedLDAModel ldaModel = (DistributedLDAModel)(new LDA().setK(10).setMaxIterations(100).run(corpus));
		
		System.out.println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize() + " words):");
		Matrix topics = ldaModel.topicsMatrix();
		for (int topic = 0; topic < 10; topic++) {
			System.out.println("Topic " + topic + ":");
			for (int word = 0; word < ldaModel.vocabSize(); word++) {
				System.out.println(vocabulary[word] + ":\t" + topics.apply(word, topic));
			}
		}
		
		jsc.close();
		return new LdaVocab(vocabulary,ldaModel);
	}
	
	public static void testOneDoc(String path, LdaVocab lv) throws IOException {
		Vector documentVector = SupportClass.loadNewDocumentAsLdaInput(path, lv.getVocab());
		Vector newDocumentDistribution = lv.getModel().toLocal().topicDistribution(documentVector);
		double[] topicDistribution = newDocumentDistribution.toArray();
		double sum = 0.0;
		for(int d =0 ; d<topicDistribution.length; ++d) {
			System.out.println(d + "\t" + topicDistribution[d]);
			sum+=topicDistribution[d];
		}
		System.out.println(sum);
	}
	


	public static Dataset<Row> removeStopWords(String path,int[] fileIndex) throws IOException {
		SparkSession spark = SparkSession.builder().appName("JavaPipelineExample").master("local[2]").getOrCreate();
		StopWordsRemover remover = new StopWordsRemover().setInputCol("raw").setOutputCol("NoStopWords");

		ArrayList<Row> data = SupportClass.getTrainingAsRowFilter(path,fileIndex);
		StructType schema = new StructType(new StructField[] {
				new StructField("id", DataTypes.LongType, false, Metadata.empty()),
				new StructField("raw", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty()) });

		Dataset<Row> dataset = spark.createDataFrame(data, schema);
		Dataset<Row> afterFiltered = remover.transform(dataset).select("NoStopWords");
		return afterFiltered;
	}
	
	public static List<Vector> getVectorForLdaInput(Dataset<Row> wordCount,int vocabularySize){
		List<Vector> lv = new ArrayList<Vector>();
		for(Row r : wordCount.collectAsList()) {
			String element2 = r.toString().split("\\[")[2];
			String indexContent = element2.substring(0, element2.length() -2);
			String[] indexAsString = indexContent.split(",");
			int[] index = new int[indexAsString.length];
			for(int i =0; i < indexAsString.length ; ++i) {
				index[i] = Integer.parseInt(indexAsString[i]);
			}
			
			String element3 = r.toString().split("\\[")[3];
			String countContent = element3.substring(0, element3.length() -3);
			String[] countAsString = countContent.split(",");
			double[] countNotOrder = new double[countAsString.length];
			for(int i =0; i < countAsString.length ; ++i) {
				countNotOrder[i] = Double.parseDouble(countAsString[i]);
			}

			double[] count = new double[vocabularySize];
			for(int i=0 ; i < indexAsString.length ; ++i) {
				count[index[i]] = countNotOrder[i];
			}
			lv.add(Vectors.dense(count));
		}
		return lv;
	}
}
