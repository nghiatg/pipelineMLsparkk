package MainPackage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;

public class JavaDocument implements Serializable {

	private long id;
	private String text;

	public JavaDocument(long id, String text) {
		this.id = id;
		this.text = text;
	}
	
	public JavaDocument(long id,File file) throws IOException {
		StringBuilder builder = new StringBuilder();
		BufferedReader br = new BufferedReader(new FileReader(file.getAbsolutePath()));
		String line = br.readLine();
		while(line != null) {
			builder.append(line + " ");
			line = br.readLine();
		}
		this.id = id;
		this.text = builder.toString();
		br.close();
	}

	public long getId() {
		return this.id;
	}

	public String getText() {
		return this.text;
	}
	
	
	
}
