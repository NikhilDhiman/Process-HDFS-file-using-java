package com.nikhil;

//Importing Libraries
//Java libraries
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

//Hadoop Libraries
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/*
RESPONSIBILITIES:- This is the main class containing the main function as the entry point of the program
This class mainly read and process the file stored on HDFS and the saves the aggregated dato to Mysql
using SaveToMysql class functions.
*/
public class ReadProcessHdfsFile {

	public static void main(String[] args) {

		System.out.print("Reading HDFS File...");

		//Creating a object of Configuration class.
		Configuration conf = new Configuration();
		try {
			final String uriString = "hdfs://localhost:9000/"; 
			final String directoryName = "inputRawData/";

			//Getting a file system instanace based on the uri provided.
			FileSystem fs = FileSystem.get(new URI(uriString), conf);

			//Creating a array of type FileStatus to store all the files or directory in the given directory in HDFS.
			FileStatus[] filesInDirectory = fs.listStatus(new Path(uriString + directoryName));

			//Checking if the provided directory contain a file or not, if not exit the program.
			if(filesInDirectory.length == 0) {
				System.out.println("No file in the directory: " + directoryName);
				System.exit(1);
			}

			//Getting the path of the first file present in the provided directory. 
			//For now processing only on the single file i.e first file in the FileStatus array.
			Path filePath = filesInDirectory[0].getPath();

			System.out.println("Processing HDFS File...");

			//Opening and reading the file from the provided path.
			FSDataInputStream in = fs.open(filePath);

			//Creating a instance of BufferedReader to read the text from a character-based input stream.
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
			String line = null;

			//Creating a ArrayList of type String as we don't know the total number of lines or the size of array.
			ArrayList<String> linesArrayList = new ArrayList<String>();

			//Reading text line by line and adding them in lineArrayList.
			while ((line=bufferedReader.readLine())!=null){
				linesArrayList.add(line);
			}

			//Initializing arrays to store age and city column data
			//As we know the total size of lineArrayList, we can create arrays no need of arrayList.
			int[] ageArray = new int[linesArrayList.size()];
			String[]  cityArray = new String[linesArrayList.size()];

			//Looping of LinesArrayList to fill data in ageArray and cityArray
			for(int index=0; index < linesArrayList.size(); index++) {
				//Spliting each line based on "," and storing data in lineSplitArray
				String[] lineSplitsArray = linesArrayList.get(index).split(",");
				ageArray[index] = Integer.parseInt(lineSplitsArray[1]);
				cityArray[index] = lineSplitsArray[2];
			}

			//Calling function findMedian to calculate the median of ageArray.
			double ageMedian = findMedian(ageArray, ageArray.length);

			//Calling function findMode to calculate the mode of cityArray
			String cityMode = mode(cityArray);

			System.out.println("Median of age column: " + ageMedian);
			System.out.println("Mode of city column: " + cityMode);

			//Creating a object of SaveToMysql Class to store data on MYSQL Table
			SaveToMysql saveToMysql = new SaveToMysql();
			//inserting record to mysql table.
			saveToMysql.insertRecordToTable(ageMedian, cityMode);

			//Close InputStream
			in.close();      
		} catch (IOException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}


	}

	/*
	 RESPONSIBILITIES:- Static function to calculate the median of the given array.
	 PARAMETERS:- 1. Array of int type, 2. Size of the array
	 RETURN:- Function returns the calculated median of double type.  
	 */
	public static double findMedian(int arrInput[], int size){
		// Sort the array
		Arrays.sort(arrInput);

		//Checking for even case
		if (size % 2 != 0)
			return (double)arrInput[size/2];
		return (double)(arrInput[(size-1)/2] + arrInput[size/2])/2.0;
	}

	/*
	 RESPONSIBILITIES:- Static function to calculate the mode of the given array.
	 PARAMETERS:- 1. Array of String type
	 RETURN:- Function returns the calculated mode of String type.  
	 */
	public static String mode(String []array){
		//Creating the HashMap to store <key, value> pair of String and integer type resp.
		HashMap<String,Integer> hashMapPairs = new HashMap<String,Integer>();
		int max  = 1;
		String temp = "";

		for(int i = 0; i < array.length; i++) {
			if (hashMapPairs.get(array[i]) != null) {
				int count = hashMapPairs.get(array[i]);
				count++;
				hashMapPairs.put(array[i], count);
				if(count > max) {
					max  = count;
					temp = array[i];
				}
			}
			else 
				hashMapPairs.put(array[i],1);
		}
		return temp;
	}

}
