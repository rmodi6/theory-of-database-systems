import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class SalaryStdDev {

	private static Connection connection = null;

	/**
	 * Create connection to database
	 *
	 * @param url      db2 jdbc url
	 * @param user     db2 username
	 * @param password db2 password
	 */
	public void setDBConnection(String url, String user, String password) {
		try {
			Class.forName("com.ibm.db2.jcc.DB2Driver");
			connection = DriverManager.getConnection(url, user, password);
		} catch (Exception e) {
			System.out.println("Error creating connection to database: " + e.getMessage());
		}
	}

	/**
	 * Execute query and print standard deviation
	 *
	 * @param sql SQL query to be executed
	 * @throws SQLException Error while executing sql statements
	 */
	public static void printQueryResult(String sql) throws SQLException {
		Statement statement = null;
		ResultSet resultSet = null;
		try {
			statement = connection.createStatement();
			resultSet = statement.executeQuery(sql);

			// Fetch results into salary list
			List<Double> salaryList = new ArrayList<>();
			while (resultSet.next()) {
				salaryList.add(resultSet.getDouble("salary"));
			}

			// Output standard deviation of the salary list
			System.out.println("Standard deviation of salary: " + calculateStandardDeviation(salaryList));
		} catch (SQLException e) {
			System.out.println("Error executing query: " + e.getMessage());
		} finally {
			statement.close();
			resultSet.close();
		}

	}

	/**
	 * Compute standard deviation of a list of numbers
	 *
	 * @param list list of numbers
	 * @return standard deviation of the list
	 */
	private static double calculateStandardDeviation(List<Double> list) {
		int n = list.size(); // number of elements in the list
		double avg = list.stream().mapToDouble(x -> x).sum() / n; // average value of the list
		// Compute and return the standard deviation using formula sqrt(sum((X - mu)^2) / n)
		return Math.pow(list.stream().mapToDouble(x -> Math.pow(x - avg, 2)).sum() / n, 0.5);
	}

	/**
	 * Main method
	 *
	 * @param args command line arguments
	 */
	public static void main(String[] args) {
		try {
			String dbURL = "jdbc:db2://localhost:50000/%s";
			if (args.length == 4) {
				// Store command line arguments into variables
				String databasename = args[0];
				String tablename = args[1];
				String user = args[2];
				String password = args[3];

				// Create db2 connection url adding databasename
				dbURL = String.format(dbURL, databasename);

				// Create SQL query to be executed adding tablename
				String sql = String.format("select salary from %s", tablename);

				SalaryStdDev salaryStdDev = new SalaryStdDev();
				salaryStdDev.setDBConnection(dbURL, user, password);
				printQueryResult(sql);
			} else {
				throw new Exception("Usage: java -cp '.:path/to/db2jcc4.jar' SalaryStdDev databasename tablename user password");
			}

		} catch (Exception e) {
			System.out.println("Error executing code: " + e.getMessage());
		}
	}

}