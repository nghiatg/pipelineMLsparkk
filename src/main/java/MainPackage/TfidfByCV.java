package MainPackage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class TfidfByCV {
	
	//save the vocabulary in ...
	private String[] vocabulary;
	
	//save the tfidf result in ...
	private ArrayList<double[]> vectors;
	
	public TfidfByCV(String path,int[] filter) throws IOException {
		vectors = new ArrayList<double[]>();
		tfByCVWithFilter(path,filter);
	}
	
//	public TfidfByCV(String path,int[] filter) throws IOException {
//		if(filter == null) {
//			vectors = new ArrayList<double[]>();
//			tfByCV(path);
//		}else {
//			vectors = new ArrayList<double[]>();
//			tfByCVWithFilter(path,filter);
//		}
//	}
	
	public String[] getVocabulary() {
		return this.vocabulary;
	}
	
	public ArrayList<double[]> getVectors(){
		return this.vectors;
	}
	
	public void tfByCVWithFilter(String path,int[] filter) throws IOException {
		SparkSession spark = SparkSession.builder().appName("JavaPipelineExample").master("local[2]").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		StructType schema = new StructType(new StructField[] {
				new StructField("id", DataTypes.LongType, false, Metadata.empty()),
				new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty()) });

		ArrayList<Row> trainingRows = SupportClass.getTrainingAsRowFilter(path,filter);
		Dataset<Row> df = spark.createDataFrame(trainingRows, schema);

		StopWordsRemover remover = new StopWordsRemover().setInputCol("text").setOutputCol("NoStopWords");
		Dataset<Row> afterRemoveST = remover.transform(df);
		afterRemoveST.cache();
		// fit a CountVectorizerModel from the corpus
		CountVectorizerModel cvModel = new CountVectorizer().setInputCol("NoStopWords").setOutputCol("features")
				.setVocabSize(20000)
				.fit(afterRemoveST);
		Dataset<Row> afterCV = cvModel.transform(afterRemoveST);
		afterCV.cache();
		IDF idf = new IDF().setInputCol(cvModel.getOutputCol()).setOutputCol("final");
		IDFModel idfModel = idf.fit(afterCV);
		Dataset<Row> rs  = idfModel.transform(afterCV);
		rs.cache();
		vocabulary = cvModel.vocabulary();
				
		List<Row> rowList = rs.select("final").collectAsList();
		System.err.println(rowList.size());
		
		for(Row r : rowList) {
			double[] vector = new double[vocabulary.length];
			
			String rowAsString = r.toString();
			String[] elements = rowAsString.split("\\[");
			String element1 = elements[2].substring(0, elements[2].length()-3);
			String[] elementOf1 = element1.split(",");
			String element2 = elements[3].substring(0, elements[3].length()-3);
			String[] elementOf2 = element2.split(",");
			for(int i=0; i < elementOf2.length ; ++i) {
				vector[Integer.parseInt(elementOf1[i])] = Double.parseDouble(elementOf2[i]);
			}
			
			vectors.add(vector);
		}
	
	}
//	public static void main(String[] args) throws IOException {
//		TfidfByCV  tf =new TfidfByCV("datatrain", null);
//		System.out.println(tf.getVectors().size());
//	}
	
}
