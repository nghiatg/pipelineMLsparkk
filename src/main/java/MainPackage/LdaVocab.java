package MainPackage;

import java.io.Serializable;

import org.apache.spark.mllib.clustering.DistributedLDAModel;

public class LdaVocab implements Serializable{
	private DistributedLDAModel ldaModel;
	private String[] vocabulary;
	public LdaVocab(String[] vocab, DistributedLDAModel model) {
		this.vocabulary = vocab;
		this.ldaModel = model;
	}
	public DistributedLDAModel getModel() {
		return this.ldaModel;
	}
	public String[] getVocab() {
		return this.vocabulary;
	}
}
