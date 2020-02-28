import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class SalaryStdDev {

	private static Connection con = null;

	public void setDBConnection(String url, String user, String password) {
		try {
			Class.forName("com.ibm.db2.jcc.DB2Driver");
			con = DriverManager.getConnection(url, user, password);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void printQueryResult(String sql) throws SQLException {
		Statement statement = null;
		ResultSet resultSet = null;
		try {
			statement = con.createStatement();
			resultSet = statement.executeQuery(sql);

			List<Double> salaryList = new ArrayList<>();
			while (resultSet.next()) {
				salaryList.add(resultSet.getDouble("salary"));
			}

			System.out.println("Standard deviation of salary: " + calculateStandardDeviation(salaryList));
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			statement.close();
			resultSet.close();
		}

	}

	private double calculateStandardDeviation(List<Double> list) {
		int n = list.size();
		double avg = list.stream().mapToDouble(x -> x).sum() / n;
		return Math.pow(list.stream().mapToDouble(x -> Math.pow(x - avg, 2)).sum() / n, 0.5);
	}

	public static void main(String[] args) {
		try {
			String dbURL = "jdbc:db2://localhost:50000/%s";
			if (args.length == 4) {
				String databasename = args[0];
				String tablename = args[1];
				String user = args[2];
				String password = args[3];

				dbURL = String.format(dbURL, databasename);

				String sql = "select salary from %s";
				sql = String.format(sql, tablename);
				SalaryStdDev salaryStdDev = new SalaryStdDev();
				salaryStdDev.setDBConnection(dbURL, user, password);
				salaryStdDev.printQueryResult(sql);
			} else {
				throw new Exception("Usage: java SalaryStdDev databasename tablename user password");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}