package com.nikhil;

//Java Libraries
import java.sql.*;

/*
RESPONSIBILITIES:- This class handles the connection to MYSQL and insertion of data in MYSQL table.
*/
public class SaveToMysql {

	private Connection connection;

	//Create the mysql connection as soon as class object is created.
	public SaveToMysql(){
		createMysqlConnection();
	}

	/*
	 RESPONSIBILITIES:- public function to insert record in mysql table. 
	 Also it closes the connection after inserting the record as over here we are only 
	 required to insert a single record. BTW we can call createMysqlConnetion to setup connection again if required
	 PARAMETERS:- 1. age median of double type, and 2. city mode of String type 
	 */
	public void insertRecordToTable(double ageMedian, String cityMode){
		try {
			System.out.println("Saving data to Mysql...");
			//Creating a Mysql Query
			String query = " insert into aggregateUserData (ageMedian, cityMode)"
					+ " values (?, ?)";

			//Creating the mysql insert preparedstatement
			PreparedStatement preparedStmt = connection.prepareStatement(query);
			preparedStmt.setDouble(1, ageMedian);
			preparedStmt.setString(2, cityMode);

			// execute the preparedstatement
			preparedStmt.execute();	      
			System.out.println("Record saved to Mysql table.");
		}catch(Exception e) {
			System.err.println("Got an exception! While saving data on Mysql");
			System.err.println(e.getMessage());
		}finally{
			closeMysqlConnection();
		}
	}

	/*
	 RESPONSIBILITIES:- public function to create a connection to MYSQL.
	 */
	public void createMysqlConnection() {
		System.out.println("Creating Connection to Mysql");
		try {
			String driver = "com.mysql.cj.jdbc.Driver";
			String mysqlUrl = "jdbc:mysql://localhost:3306/processedData";
			final String userName = "localUser";
			final String password = "Nikhil@123";
			Class.forName(driver);
			connection = DriverManager.getConnection(mysqlUrl, userName, password);
		}catch(Exception e) {
			System.err.println("Got an exception! While connecting to MYSQL");
			System.err.println(e.getMessage());
		}
	}

	/*
	 RESPONSIBILITIES:- public function to close connection to MYSQL.
	 */
	public void closeMysqlConnection() {
		System.out.println("Closing Connection to Mysql...");
		try {
			connection.close();
			System.out.println("Connection Closed to Mysql");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}


}
