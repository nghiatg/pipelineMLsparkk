package MainPackage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

public class URL {

	public static HashMap<String,Double> urlOneSide(String path, HashMap<Long,Integer> gender, boolean m) throws IOException{
		HashMap<String,int[]> uwg = urlWithGender(path,gender);
		return urlOneSide(uwg, m);
	}
	
	public static HashMap<String,Double> urlOneSide(HashMap<String,int[]> urlWithGender, boolean m){
		HashMap<String,Double> result = new HashMap<String,Double>();
		Set<String> paths = urlWithGender.keySet();
		if(m == true) {
			for(String path : paths) {
				int[] mf = urlWithGender.get(path);
				if((double)mf[0] / ((double)mf[1] + (double)mf[0])  > 0.8) {
					result.put(path,(double)mf[0] / ((double)mf[1] + (double)mf[0]));
				}
			}
		}else {
			for(String path : paths) {
				int[] mf = urlWithGender.get(path);
				if((double)mf[1] / ((double)mf[1] + (double)mf[0]) > 0.8) {
					result.put(path,(double)mf[1] / ((double)mf[1] + (double)mf[0]));
				}
			}
		}
		return result;
	}
	
	public static HashMap<String,int[]> urlWithGender(String path, HashMap<Long,Integer> gender) throws IOException {
		HashMap<String,int[]> result = new HashMap<String,int[]>();
		File parentFile = new File(path);
		File[] subfiles = parentFile.listFiles(new FileFilter() {

			@Override
			public boolean accept(File pathname) {
				if(!pathname.isHidden()) {
					return true;
				}
				return false;
			}
		});
		for(File f : subfiles) {
			combine(result, urlOneFile(f,gender));
		}
		
		return result;
	}
	
	public static void combine(HashMap<String,int[]> inAll, HashMap<String,int[]> inOneFile ) {
		Set<String> oneFileKeys = inOneFile.keySet();
		for(String path : oneFileKeys) {
			if(!inAll.containsKey(path)) {
				inAll.put(path,inOneFile.get(path));
			}else {
				int[] mfAll = inAll.get(path);
				int[] mfOne = inOneFile.get(path);
				mfAll[0]+=mfOne[0];
				mfAll[1]+=mfOne[1];
				inAll.put(path, mfAll);
			}
		}
	}
	
	public static HashMap<String,int[]> urlOneFile(File f,HashMap<Long,Integer> gender) throws IOException {
		HashMap<String,int[]> result = new HashMap<String,int[]>();
		BufferedReader br = new BufferedReader(new FileReader(f));
		String line = br.readLine();
		while(line != null) {
			String path = "";
			long guid = 0;
			int pos = 0, end;
			try {
				for (int r = 0; r < 14; r++) {
					end = line.indexOf('\t', pos);
					if(r == 11) {
						path = line.substring(pos, end);
					}else if(r == 13) {
						guid = Long.parseLong(line.substring(pos, end));
					}
					pos = end + 1;
				}
				if(gender.get(guid) == 1) {
					if(result.keySet().contains(path)) {
						int[] mf = result.get(path);
						mf[0]++;
						result.put(path, mf);
					}else {
						int[] mf = new int[] {1,0};
						result.put(path, mf);
					}
				}else if(gender.get(guid) == -1) {
					if(result.keySet().contains(path)) {
						int[] mf = result.get(path);
						mf[1]++;
						result.put(path, mf);
					}else {
						int[] mf = new int[] {0,1};
						result.put(path, mf);
					}
				}
			} catch (Exception e) {
				line = br.readLine();
				continue;
			}
			
			line = br.readLine();
		}
		br.close();
		return result;
	}
	
	public static HashMap<Long,Integer> createGenderSample(String path) throws IOException{
		HashMap<Long,Integer> result = new HashMap<Long,Integer>();
		BufferedReader br = new BufferedReader(new FileReader(path));
		String line = br.readLine();
		while(line != null) {
			long guid = 0;
			int pos = 0, end;
			try {
				for (int r = 0; r < 14; r++) {
					end = line.indexOf('\t', pos);
					if(r == 13) {
						guid = Long.parseLong(line.substring(pos, end));
					}
					pos = end + 1;
				}
			} catch (Exception e) {
				line = br.readLine();
				continue;
			}
			
			if(Math.random() > 0.5) {
				if(Math.random() > 0.5) {
					result.put(guid,1);
				}else {
					result.put(guid,-1);
				}
			}
			line = br.readLine();
		}
		br.close();
		return result;
	}
}
