package MainPackage;

import java.io.IOException;
import java.util.Arrays;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
// $example off$
import org.apache.spark.sql.SparkSession;

/**
 * Java example for simple text document 'Pipeline'.
 */
public class TfidfPipelineExample {
	public static void runTFIDF(String path) throws IOException {
//		System.setProperty("hadoop.home.dir", "D:\\winutils\\hadoop");
		SparkSession spark = SparkSession.builder().appName("JavaPipelineExample").master("local[2]").getOrCreate();
		
		Dataset<Row> training = spark.createDataFrame(SupportClass.getTrainingFilesFromDirectory(path),JavaDocument.class);
		training.cache();
		
		
		Tokenizer tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words");
		HashingTF hashingTF = new HashingTF().setInputCol(tokenizer.getOutputCol()).setOutputCol("features");
		IDF idf = new IDF().setInputCol(hashingTF.getOutputCol()).setOutputCol("final");
		Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] { tokenizer, hashingTF, idf });
		
		
		PipelineModel model = pipeline.fit(training);
		Dataset<Row> test = spark.createDataFrame(SupportClass.getTestFilesFromDirectory(path),JavaDocument.class);

		Dataset<Row> rs = model.transform(test);
//		System.out.println(rs.select("final").first().toString());;
		rs.show(false);
		spark.stop();
		
		
		
	}
}