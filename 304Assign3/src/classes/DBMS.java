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
				
				switch (commands[0]) {
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
					System.out.println("Connection closed");;
					} catch (SQLException sqe) {
						sqe.getMessage();
					}
					break;
				case "load":
					if (commandLength > 1) { break; }
					loadFile();
					break;
				case "insert":
					break;
				case "delete":
					break;
				case "modify":
					break;
				default: 
					continue;
				
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} 
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
