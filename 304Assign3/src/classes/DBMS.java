package classes;
import java.io.*;
import java.sql.*;
import java.util.Scanner;

public class DBMS {
	final static int MAX_LEN = 255;
	static Connection connection = null;
	static Statement statement = null;

	public static void main(String[] args) {
		printInstructions();
		try {
			// simulate command line to get user input
			for (int prompt = 1; prompt > 0;) {
				System.out.print("dbms> ");
				BufferedReader userInput = new BufferedReader(new InputStreamReader(System.in));
				
				String commandLine = userInput.readLine();
				if (commandLine == null) { continue; }
				if (commandLine.length() > MAX_LEN) { continue; }
				String[] commands = commandLine.split(" ");
//				printCommands(commands);
				int commandLength = commands.length;
				
				switch (commands[0].toLowerCase()) {
					case "connect":
//					if (commandLength != 3) { break; }
						try {
//						connection = connectToJDBC(commands[1], commands[2]);
							connection = connect();
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
						insert(commands);
						break;
					case "delete":
						break;
					case "update":
						break;
					case "select":
						String query = getQueryString(commands);
						ResultSet results = executeQuery(query);
						printResults(results);
						break;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}

	private static void printResults(ResultSet results) {
		try {
			ResultSetMetaData metaData = results.getMetaData();
			System.out.println(metaData.getTableName(1));

			//assumed to be a max size as examples don't add a lot of tuples
			String[][] table = new String[50][];
			String columnHeaders = "";
			for (int i = 1; i < metaData.getColumnCount(); i++) {
				columnHeaders += (metaData.getColumnName(i) + " ");
			}
			String[] headers = columnHeaders.split(" ");
			table[0] = headers;

			while (results.next()) {
				String row = "";
				for (int i = 1; i < metaData.getColumnCount(); i++) {
					row += (results.getString(i) + " ");
					String[] rowData = row.split(" ");
					table[i] = rowData;
				}
			}

			for (final String[] row : table) {
				System.out.format("%15s%15s%15s\n", row);
			}


		} catch (SQLException sqe) {
			sqe.getMessage();
		}
	}

	/**
	 * adds a tuple of the given entity with the given attributes if the query is valid
	 * and does not already exist in the relation
	 * @param commands
	 */
	private static void insert(String[] commands) {
		String query = getQueryString(commands);
		if (!alreadyAdded(commands)) {
			int rows = executeStatement(query);
			if (rows == 1) {
				System.out.println("Entry successfully added");
			}
		} else {
			System.out.println("Entry was not added: duplicate");
		}
	}

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
		String values = commands[3];
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
					executeStatement(query);
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
	private static int executeStatement(String query) {
		int rows = 0;
		try {
			rows = statement.executeUpdate(query);
		} catch (SQLException sqe) {
			sqe.getMessage();
		}
		return rows;
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
	 * connection for testing only TODO: delete when finished
	 * @return
	 * @throws SQLException
	 */
	private static Connection connect() throws SQLException {
		DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
		return DriverManager.getConnection(
				"jdbc:oracle:thin:@dbhost.ugrad.cs.ubc.ca:1522:ug", "ora_w5x9a", "a16179146");
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
				+ "\n[2] load <filename>"
				+ "\n    Load data to DB with .sql file."
				+ "\n[3] "
				+ "\n[4] "
				+ "\n[5] "
				+ "\n[6] close"
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
