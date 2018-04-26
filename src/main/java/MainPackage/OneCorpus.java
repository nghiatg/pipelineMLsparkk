package MainPackage;

import java.util.ArrayList;



//need to rename this class
public class OneCorpus {
	private ArrayList<double[]> vectors;
	private ArrayList<Integer> vectorID;
	private OneCorpus firstChild;
	private OneCorpus secondChild;
	private double[] centroid;
	private int total;
	
	public OneCorpus(ArrayList<double[]> v,int totalCorpusSize,ArrayList<Integer> ids) {
		this.total = totalCorpusSize;
		this.vectors = v;
		this.vectorID = ids;
		setCentroid();
		if(vectors.size() > 1) {
			basicKmean();
		}
	}
	
	public int getLayer() {
		return this.total;
	}
	
	public ArrayList<double[]> getVectors(){
		return this.vectors;
	}
	
	public void addVector(double[] element) {
		this.vectors.add(element);
	}
	
	public void setCentroid() {
		this.centroid = SupportClass.getCentroidAtUnitLength(vectors);
	}
	
	public boolean arraysEqual(double[] firstArray, double[] secondArray) {
		if(firstArray.length == secondArray.length) {
			for(int index =0 ; index < firstArray.length ; index++) {
//				if((int)Math.floor(firstArray[index]) != (int)Math.floor(secondArray[index])) {
//					return false;
//				}
				if(firstArray[index] != secondArray[index]) {
					return false;
				}
			}
			return true;
 		}
		return false;
	}
	
	public void basicKmean() {
		if(vectors.size() > total/10) {
			ArrayList<Integer> firstAreaVectorID = new ArrayList<Integer>();
			ArrayList<Integer> secondAreaVectorID = new ArrayList<Integer>();
			double[] firstCenter = new double[vectors.get(0).length];
			double[] secondCenter = new double[vectors.get(0).length];
			double[] updateFirstCenter = vectors.get((int)(Math.random() * vectors.size()));
			double[] updateSecondCenter = findSymmetricVector(updateFirstCenter);
			ArrayList<double[]> firstArea = new ArrayList<double[]>();
			ArrayList<double[]> secondArea = new ArrayList<double[]>();
			while((!arraysEqual(firstCenter,updateFirstCenter)) || (!(arraysEqual(secondCenter,updateSecondCenter)))) {
				int equalDis = 0;
				firstArea.clear();
				secondArea.clear();
				firstAreaVectorID.clear();
				secondAreaVectorID.clear();
				firstCenter = updateFirstCenter;
				secondCenter = updateSecondCenter;
				for(int i = 0 ; i< vectors.size();++i) {
					double[] vector = vectors.get(i);
					if(SupportClass.getDistance(updateFirstCenter, vector) < SupportClass.getDistance(updateSecondCenter,vector)) {
						firstArea.add(vector);
						firstAreaVectorID.add(vectorID.get(i));
					}else if (SupportClass.getDistance(updateFirstCenter, vector) > SupportClass.getDistance(updateSecondCenter,vector)){
						secondArea.add(vector);
						secondAreaVectorID.add(vectorID.get(i));
					}else {
						if(equalDis == 0) {
							firstArea.add(vector);
							firstAreaVectorID.add(vectorID.get(i));
						}else {
							secondArea.add(vector);
							secondAreaVectorID.add(vectorID.get(i));
						}
						equalDis = 1-equalDis;
					}
				}
				updateFirstCenter = SupportClass.getCentroidAtUnitLength(firstArea);
				updateSecondCenter = SupportClass.getCentroidAtUnitLength(secondArea);
			}
			System.out.println(firstArea.size() + "\t" + secondArea.size());
			firstChild = new OneCorpus(firstArea, total,firstAreaVectorID);
			secondChild = new OneCorpus(secondArea, total,secondAreaVectorID);
		}else {
			firstChild = null;
			secondChild = null;
		}
	}
	
	public double[] findSymmetricVector(double[] basedVector) {
		double[] normalizedBasVector = SupportClass.normalize(basedVector);
		return findSymmetricPoint(normalizedBasVector);
	}
	
	public double[] findSymmetricPoint(double[] basedVector) {
		int size = centroid.length;
		double[] expected = new double[size];
		double[] result = null;
		int index = 0;
		while(index < size) {
			expected[index] = centroid[index] + centroid[index] - basedVector[index];
			index++;
		}
		
		double distancePivot = SupportClass.getDistance(vectors.get(1),expected);
		
		for(double[] vector : vectors) {
			if(SupportClass.getDistance(vector,expected) <= distancePivot) {
				distancePivot = SupportClass.getDistance(vector,expected);
				result = vector;
			}
		}
		return result;
	}
	
	
	public ArrayList<int[]> getAllLeafVectorID() {
		ArrayList<int[]> result = new ArrayList<int[]>();
		if(firstChild == null && secondChild == null) {
			result.add(convert(vectorID));
		}else {
			result.addAll(firstChild.getAllLeafVectorID());
			result.addAll(secondChild.getAllLeafVectorID());
		}
		return result;
	}
	
	public int[] convert(ArrayList<Integer> list) {
		int[] result = new int[list.size()];
		for(int index = 0 ; index < list.size(); ++index) {
			result[index] = list.get(index);
		}
		return result;
	}
	
}
