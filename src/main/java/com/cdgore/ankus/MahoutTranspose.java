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
import org.apache.mahout.math.hadoop.TransposeJob;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class MahoutTranspose extends AbstractJob {
	private Map<String, Integer> userItemMatrixDimensions = new HashMap<String, Integer>();
	private Map<String, Integer> userItemMatrixDimensions2 = new HashMap<String, Integer>();

	public MahoutTranspose() {
		// TODO Auto-generated constructor stub
	}

	public MahoutTranspose(Configuration conf) {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode = ToolRunner.run(new MahoutTranspose(), args);
		System.exit(exitCode);
	}

	public int run(String[] arg0) throws Exception {
		addInputOption();
		addOutputOption();
	    addOption("numRows", "nra", "Number of rows of the input matrix");
	    addOption("numCols", "nca", "Number of columns of the input matrix");
	    addOption("dimJson", "dj", "Path to Json with values for number of rows and/or number of items");
	    addOption("dimJsonNumRows", "djnr", "Name of the field for the number of rows in the aforementioned Json");
	    addOption("dimJsonNumCols", "djnc", "Name of the field for the number of columns in the aforementioned Json");
	    addOption("dimJson2", "dj2", "Path to Json with values for number of rows and/or number of items");
	    addOption("dimJson2NumRows", "dj2nr", "Name of the field for the number of rows in the aforementioned Json");
	    addOption("dimJson2NumCols", "dj2nc", "Name of the field for the number of columns in the aforementioned Json");
	    addOption("reduceTasks", "rt", "Number of reduce tasks to use");
	    Map<String, List<String>> parsedArgs = parseArguments(arg0);
	    if (parsedArgs == null) {
	      return -1;
	    }
	    
		if (!hasOption("numRows") && !hasOption("dimJson") && !hasOption("dimJsonNumRows"))
			throw new Exception("ERROR: Must specify the number of rows either by --numRows or with a Json by specifying "
							+ "--dimJson for the Json's path and --dimJsonNumRows for the field that specifies the number of rows");
	    
		int numRows = 0;
		int numCols = 0;
		
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
					userItemMatrixDimensions = dimGson.fromJson(line, new TypeToken<Map<String, Integer>>() {}.getType());
					br.close();
				}
			} catch (Exception e) {
				System.out.println("File not found: " + e.toString());
			}
			if (hasOption("dimJsonNumRows"))
				if (userItemMatrixDimensions.containsKey(getOption("dimJsonNumRows")))
					numRows = userItemMatrixDimensions.get(getOption("dimJsonNumRows"));
			if (hasOption("dimJsonNumCols"))
				if (userItemMatrixDimensions.containsKey(getOption("dimJsonNumCols")))
					numCols = userItemMatrixDimensions.get(getOption("dimJsonNumCols"));
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
					userItemMatrixDimensions2 = dimGson.fromJson(line, new TypeToken<Map<String, Integer>>() {}.getType());
					br.close();
				}
			} catch (Exception e) {
				System.out.println("File not found: " + e.toString());
			}
			if (hasOption("dimJson2NumRows"))
				if (userItemMatrixDimensions2.containsKey(getOption("dimJson2NumRows")))
					numRows = userItemMatrixDimensions2.get(getOption("dimJson2NumRows"));
			if (hasOption("dimJson2NumCols"))
				if (userItemMatrixDimensions2.containsKey(getOption("dimJson2NumCols")))
					numCols = userItemMatrixDimensions2.get(getOption("dimJson2NumCols"));
		}
	    if (hasOption("numRows"))
	    	numRows = Integer.parseInt(getOption("numRows"));
	    if (hasOption("numCols"))
	    	numCols = Integer.parseInt(getOption("numCols"));
//	    if (numRows == 0)
//	    	numRows = getDimensions(getInputPath());
	    
	    System.out.println("numRows: " + numRows + " numCols: " + numCols);
	    
	    DistributedRowMatrix matrix = new DistributedRowMatrix(getInputPath(), getTempPath(), numRows, numCols);
	    Configuration conf = getConf();
	    matrix.setConf(conf);
	    Configuration conf2 = matrix.getConf();
		
		Configuration transposeConf = TransposeJob.buildTransposeJobConf(conf2, inputPath, outputPath, numRows);
		Job transposeJob = new Job(transposeConf, "Matrix tranposition");
		if (hasOption("reduceTasks"))
			transposeJob.setNumReduceTasks(Integer.parseInt(getOption("reduceTasks")));
		return transposeJob.waitForCompletion(true) ? 0 : 1;
	}

}
