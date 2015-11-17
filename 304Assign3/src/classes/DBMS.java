package classes;
import java.io.*;
import java.sql.*;
import java.util.Scanner;

public class DBMS {
	static Connection connection = null;
	static Statement statement = null;

	public static void main(String[] args) {
		printInstructions();
		try {// simulate command line to get user input
			for (int prompt = 1; prompt > 0;) {
				System.out.print("dbms> ");
				BufferedReader userInput = new BufferedReader(new InputStreamReader(System.in));
				String commandLine = userInput.readLine();
				if (commandLine == null) { continue; }
				String[] commands = commandLine.split(" ");
//				printCommands(commands);
				int commandLength = commands.length;
				
				switch (commands[0].toLowerCase()) {
					case "connect":
					if (commandLength != 3) { break; }
						try {
							connection = connectToJDBC(commands[1], commands[2]);
							System.out.println("Connected");
							statement = connection.createStatement();
							System.out.println("Statement created");
						} catch (SQLException sqe) {
							System.out.println(sqe.getMessage());
							continue;
						}
						break;
					case "close":
						try {
							connection.close();
							System.out.println("Connection closed");
						} catch (SQLException sqe) {
							sqe.getMessage();
						}
						break;
					case "load":
						if (commandLength > 1) {
							break;
						}
						loadFile();
						break;
					case "insert":
						insertTuple(commands);
						break;
					case "delete":
						if (commandLength == 2) {
							executeUpdate("DELETE " + commands[1]);
						} else {
							deleteTuple(commands);
						}
						break;
					case "select":
						select(commands);
						break;
				}
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * deletes a tuple if it is present in the relation
	 * @param commands
	 */
	private static void deleteTuple(String[] commands) {
		String query = getQueryString(commands);;
		try {
		int stock = getStock(commands);
		if (stock == 0) {
			int rows = executeUpdate(query);
			if (rows == 1) {
				System.out.println("Tuple deleted");
				return;
			}
		}} catch (SQLException sqe) {
			sqe.getMessage();
		}
		System.out.println("Stock is not empty, delete failed");
	}

	/**
	 * produces the relation that is selected by the user and print's it to the terminal
	 * @param commands commands provided by user in the terminal
	 */
	private static void select(String[] commands) {
		String query = getQueryString(commands);
		ResultSet results = executeQuery(query);
		printResults(results);
	}

	/**
	 * prints the contents of a ResultSet
	 * @param results results from a query
	 */
	private static void printResults(ResultSet results) {
		try {
			ResultSetMetaData rsmd = results.getMetaData();
			int columnsNumber = rsmd.getColumnCount();
			while (results.next()) {
				for (int i = 1; i <= columnsNumber; i++) {
					if (i > 1) System.out.print(",  ");
					String columnValue = results.getString(i);
					System.out.print(columnValue + " " + rsmd.getColumnName(i));
				}
				System.out.println("");
			}
		} catch (SQLException sqe) {
			sqe.getMessage();
		} catch (NullPointerException npe) {
			System.out.println("Query failed, try again");
		}
	}

	/**
	 * adds a tuple of the given entity with the given attributes if the query is valid
	 * and does not already exist in the relation
	 * @param commands
	 */
	private static void insertTuple(String[] commands) {
		String query = getQueryString(commands);
		if (!alreadyAdded(commands)) {
			int rows = executeUpdate(query);
			if (rows == 1) {
				System.out.println("Entry successfully added");
			}
		} else {
			System.out.println("Entry was not added: duplicate");
		}
	}

	/**
	 * Get the string for the query
	 * @param commands
	 * @return
	 */
	private static String getQueryString(String[] commands) {
		String query = "";
		for (String c : commands) {
			query += (c + " ");
		}
		return query;
	}

	/**
	 * iterates through the primary key column to check for duplicate entry based on primary key
	 * @param commands
	 * @return
	 */
	private static boolean alreadyAdded(String[] commands) {
		boolean added = false;
		String relation = commands[2];
		String values = "";
		if (commands[0].toLowerCase().equals("insert")) {
			values = commands[3];
			String primaryKey = extractPrimaryKey(values);
			ResultSet results = executeQuery("SELECT * FROM " + relation);
			try {
				while (results.next()) {
					if (results.getString(1).equals(primaryKey)) {
						return true;
					}
				}
			} catch (SQLException sqe) {
				sqe.getMessage();
			}
//		} else if (commands[0].toLowerCase().equals("delete")) {
//			for (int i = 3; i < commands.length; i++) {
//				values += commands[i];
//			}
		}

		return added;
	}

	/**
	 * extract the primary key from values
	 * @param valueString
	 * @return
	 */
	private static String extractPrimaryKey(String valueString) {
		String[] values = valueString.split(",");
		String[] result = values[0].split("'");
		return result[1];
	}
	
	/**
	 * populate the SQL DB with the statements provided by the assignment
	 */
	private static void loadFile() {
		try {
			String create = new Scanner(new FileInputStream(
					new File("create.txt"))).useDelimiter("\\Z").next();
			String data = new Scanner(new FileInputStream(
					new File("data.txt"))).useDelimiter("\\Z").next();
			String[] files = new String[]{create, data};
			for (String file : files) {
				String[] queries = file.split(";");
				for (String query : queries) {
					System.out.println(query);
					executeUpdate(query);
				}
			}
		} catch (FileNotFoundException e) {
			e.getMessage();
			e.printStackTrace();
		}	
	}

	/**
	 * return the results of a query
	 * @return
	 */
	private static ResultSet executeQuery(String query) {
		ResultSet results = null;
		try {
			results = statement.executeQuery(query);
		} catch (SQLException sqe) {
			sqe.getMessage();
		}
		return results;
	}

	/**
	 * returns the number of rows processed
	 * executes the SQL statement given by the string
	 * @param query sql statement
	 */
	private static int executeUpdate(String query) {
		int rows = 0;
		try {
			rows = statement.executeUpdate(query);
		} catch (SQLException sqe) {
			sqe.getMessage();
		}
		return rows;
	}

	/**
	 * get the stock number for a specific item
	 * @param commands
	 * @return
	 */
	private static int getStock(String[] commands) throws SQLException {
		String relation = commands[1];
		String key = commands[3];
		String value = commands[5];
		ResultSet results = executeQuery("SELECT * FROM " + relation +
				" WHERE " + key + " = " + value);
		results.next();
		return results.getInt("stock");
	}

	/**
	 * Register the JDBC Driver and return a connection to JDBC
	 * @param username
	 * @param password
	 * @throws SQLException 
	 */
	private static Connection connectToJDBC(String username, String password) throws SQLException {
		DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
		return DriverManager.getConnection(
				  "jdbc:oracle:thin:@dbhost.ugrad.cs.ubc.ca:1522:ug", username, password);
	}
	
	/**
	 * prints the opening statements and instructions for the commandline
	 */
	private static void printInstructions() {
		System.out.println("\n         Welcome to the DBMS          "
				+ "\n======================================"
				+ "\nCommands: "
				+ "\n[1] connect <username> <password>  "
				+ "\n    Connect to the JDBC."
				+ "\n[2] load"
				+ "\n    Load data to DB with .sql file."
				+ "\n[3] Any valid SQLPlus query"
				+ "\n	 Completes the operation."
				+ "\n[4] close"
				+ "\n    Close the connection to JDBC."
				+ "\n======================================"
				+ "\n\n");
	}
	
	/**
	 * prints the arguments entered into the command line
	 * @param commands
	 */
	private static void printCommands(String[] commands) {
		for (String s : commands) {
			System.out.print(s + " ");
		}
		System.out.print("\n");
	}
	
}
