package com.cdgore.ankus;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;
import org.apache.mahout.math.hadoop.MatrixMultiplicationJob;
import org.apache.mahout.math.hadoop.TransposeJob;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class MahoutMatrixMultiplication extends AbstractJob {
	private Map<String, Integer> userItemMatrixDimensions = new HashMap<String, Integer>();
	private Map<String, Integer> userItemMatrixDimensions2 = new HashMap<String, Integer>();

	public MahoutMatrixMultiplication() {
		// TODO Auto-generated constructor stub
	}

	public MahoutMatrixMultiplication(Configuration conf) {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode = ToolRunner.run(new MahoutMatrixMultiplication(), args);
		System.exit(exitCode);
	}

	public int run(String[] arg0) throws Exception {
		addOption("numRowsA", "nra", "Number of rows of the first input matrix", false);
	    addOption("numColsA", "nca", "Number of columns of the first input matrix", false);
	    addOption("numRowsB", "nrb", "Number of rows of the second input matrix", false);
	    addOption("numColsB", "ncb", "Number of columns of the second input matrix", false);
	    
	    addOption("inputPathA", "ia", "Path to the first input matrix", true);
	    addOption("inputPathB", "ib", "Path to the second input matrix", true);

	    addOption("dimJson", "dj", "Path to Json with values for number of rows and/or number of items");
	    addOption("dimJsonNumRowsA", "djnra", "Name of the field for the number of rows for matrix A in the aforementioned Json");
	    addOption("dimJsonNumColsA", "djnca", "Name of the field for the number of columns for matrix A in the aforementioned Json");
	    addOption("dimJsonNumRowsB", "djnrb", "Name of the field for the number of rows for matrix B in the aforementioned Json");
	    addOption("dimJsonNumColsB", "djncb", "Name of the field for the number of columns for matrix B in the aforementioned Json");
	    addOption("dimJson2", "dj2", "Path to Json with values for number of rows and/or number of items");
	    addOption("dimJson2NumRowsA", "dj2nra", "Name of the field for the number of rows for matrix A in the aforementioned Json");
	    addOption("dimJson2NumColsA", "dj2nca", "Name of the field for the number of columns for matrix A in the aforementioned Json");
	    addOption("dimJson2NumRowsB", "dj2nrb", "Name of the field for the number of rows for matrix B in the aforementioned Json");
	    addOption("dimJson2NumColsB", "dj2ncb", "Name of the field for the number of columns for matrix B in the aforementioned Json");
	    
	    addOption("outputPath", "op", "Path to the output matrix", true);

	    Map<String, List<String>> argMap = parseArguments(arg0);
	    if (argMap == null) {
	      return -1;
	    }
	    
	    // TODO: Throw an exception if we don't have all the requisite matrix dimension parameters
//		if (!hasOption("numRowsA") && !hasOption("dimJson") && !hasOption("dimJsonNumRowsA"))
//			throw new Exception("ERROR: Must specify the number of rows either by --numRows or with a Json by specifying "
//							+ "--dimJson for the Json's path and --dimJsonNumRows for the field that specifies the number of rows");
//	    
		int numRowsA = 0;
		int numColsA = 0;
		int numRowsB = 0;
		int numColsB = 0;
		
		if (hasOption("dimJson")) {
			Gson dimGson = new Gson();
			try {
				String dimJsonPath = getOption("dimJson");
				URI uri = URI.create(dimJsonPath);
				FileSystem fs = FileSystem.get(uri, getConf());
				FileStatus[] status = fs.listStatus(new Path(dimJsonPath));

				for (int i = 0; i < status.length; i++) {
					BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
					String line = br.readLine().trim();
					System.out.println("dimJson: " + line);
					try {
						Map<String, Integer> tempHashMap = dimGson.fromJson(line, new TypeToken<Map<String, Integer>>() {}.getType());
						userItemMatrixDimensions.putAll(tempHashMap);
					} catch (Exception e) {}
//					userItemMatrixDimensions = dimGson.fromJson(line, new TypeToken<Map<String, Integer>>() {}.getType());
					br.close();
				}
			} catch (Exception e) {
				System.out.println("File not found");
			}
			if (hasOption("dimJsonNumRowsA"))
				if (userItemMatrixDimensions.containsKey(getOption("dimJsonNumRowsA")))
					numRowsA = userItemMatrixDimensions.get(getOption("dimJsonNumRowsA"));
			if (hasOption("dimJsonNumColsA"))
				if (userItemMatrixDimensions.containsKey(getOption("dimJsonNumColsA")))
					numColsA = userItemMatrixDimensions.get(getOption("dimJsonNumColsA"));
			if (hasOption("dimJsonNumRowsB"))
				if (userItemMatrixDimensions.containsKey(getOption("dimJsonNumRowsB")))
					numRowsB = userItemMatrixDimensions.get(getOption("dimJsonNumRowsB"));
			if (hasOption("dimJsonNumColsB"))
				if (userItemMatrixDimensions.containsKey(getOption("dimJsonNumColsB")))
					numColsB = userItemMatrixDimensions.get(getOption("dimJsonNumColsB"));
		}
		if (hasOption("dimJson2")) {
			Gson dimGson = new Gson();
			try {
				String dimJsonPath = getOption("dimJson2");
				URI uri = URI.create(dimJsonPath);
				FileSystem fs = FileSystem.get(uri, getConf());
				FileStatus[] status = fs.listStatus(new Path(dimJsonPath));

				for (int i = 0; i < status.length; i++) {
					BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
					String line = br.readLine().trim();
					System.out.println("dimJson2: " + line);
					try {
						Map<String, Integer> tempHashMap = dimGson.fromJson(line, new TypeToken<Map<String, Integer>>() {}.getType());
						userItemMatrixDimensions2.putAll(tempHashMap);
					} catch (Exception e) {}
					br.close();
				}
			} catch (Exception e) {
				System.out.println("File not found");
			}
			if (hasOption("dimJson2NumRowsA"))
				if (userItemMatrixDimensions2.containsKey(getOption("dimJson2NumRowsA")))
					numRowsA = userItemMatrixDimensions2.get(getOption("dimJson2NumRowsA"));
			if (hasOption("dimJson2NumColsA"))
				if (userItemMatrixDimensions2.containsKey(getOption("dimJson2NumColsA")))
					numColsA = userItemMatrixDimensions2.get(getOption("dimJson2NumColsA"));
			if (hasOption("dimJson2NumRowsB"))
				if (userItemMatrixDimensions2.containsKey(getOption("dimJson2NumRowsB")))
					numRowsB = userItemMatrixDimensions2.get(getOption("dimJson2NumRowsB"));
			if (hasOption("dimJsonNumColsB"))
				if (userItemMatrixDimensions2.containsKey(getOption("dimJson2NumColsB")))
					numColsB = userItemMatrixDimensions2.get(getOption("dimJson2NumColsB"));
		}
		if (hasOption("numRowsA"))
	    	numRowsA = Integer.parseInt(getOption("numRowsA"));
	    if (hasOption("numColsA"))
	    	numColsA = Integer.parseInt(getOption("numColsA"));
	    if (hasOption("numRowsB"))
	    	numRowsB = Integer.parseInt(getOption("numRowsB"));
	    if (hasOption("numColsB"))
	    	numColsB = Integer.parseInt(getOption("numColsB"));
//	    if (numRows == 0)
//	    	numRows = getDimensions(getInputPath());
	    
	    System.out.println("numRowsA: " + numRowsA + " numColsA: " + numColsA +
	    		" numRowsB: " + numRowsB + " numColsB: " + numColsB);
	    
	    DistributedRowMatrix a = new DistributedRowMatrix(new Path(getOption("inputPathA")),
                new Path(getOption("tempDir")),
                numRowsA,
                numColsA);
	    DistributedRowMatrix b = new DistributedRowMatrix(new Path(getOption("inputPathB")),
                new Path(getOption("tempDir")),
                numRowsB,
                numColsB);
	    
	    a.setConf(new Configuration(getConf()));
	    b.setConf(new Configuration(getConf()));

	    if (hasOption("outputPath")) {
	      a.times(b, new Path(getOption("outputPath")));
	    } else {
	      a.times(b);
	    }

	    return 0;
	    
//	    DistributedRowMatrix matrix = new DistributedRowMatrix(getInputPath(), getTempPath(), numRows, numCols);
//	    Configuration conf = getConf();
//	    matrix.setConf(conf);
//	    Configuration conf2 = matrix.getConf();
//		
//	    Configuration matrixMultConf = MatrixMultiplicationJob.createMatrixMultiplyJobConf(conf2, inputPath, bPath, outputPath, numRows);
//		Configuration transposeConf = TransposeJob.buildTransposeJobConf(conf2, inputPath, outputPath, numRows);
//		Job transposeJob = new Job(transposeConf, "Matrix tranposition");
//		return transposeJob.waitForCompletion(true) ? 0 : 1;
	}

}
