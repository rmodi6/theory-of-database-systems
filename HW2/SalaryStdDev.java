import java.sql.*;

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
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		try {
			preparedStatement = con.prepareStatement(sql);
			resultSet = preparedStatement.executeQuery();

			while (resultSet.next()) {
				System.out.println("Standard deviation of salary: " + resultSet.getDouble("SALARYSTDDEV"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			preparedStatement.close();
			resultSet.close();
		}

	}

	public static void main(String[] args) {
		try {
			String dbURL = "jdbc:db2://172.17.0.2:50000/%s";
			if (args.length == 4) {
				String databasename = args[0];
				String tablename = args[1];
				String user = args[2];
				String password = args[3];

				dbURL = String.format(dbURL, databasename);

				String sql = "select power(sum(power(SALARY - avg(SALARY) over (order by SALARY rows between unbounded preceding and unbounded following), 2)) over (order by SALARY rows between unbounded preceding and unbounded following) / count(SALARY) over (order by SALARY rows between unbounded preceding and unbounded following), 0.5) as SALARYSTDDEV from %s limit 1";
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