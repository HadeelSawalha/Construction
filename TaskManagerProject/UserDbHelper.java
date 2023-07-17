package TaskManagerProject;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;

public class UserDbHelper {
	// To Show all users in the system
	public static ArrayList<User> showUsers() {
		ArrayList<User> users = new ArrayList<>();
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;

		try {
			con = DriverManager.getConnection("jdbc:mysql://localhost:3306/task_management_system?serverTimezone=UTC",
					"root", "password");
			stmt = con.createStatement();
			rs = stmt.executeQuery(
					"SELECT user_id, first_name, last_name, email, user_password, user_role FROM task_management_system.`user`");

			while (rs.next()) {
				User user = new User(rs.getString("first_name"), rs.getString("last_name"), rs.getString("email"),
						rs.getString("user_password"), rs.getString("user_role"));
				users.add(user);
			}
			rs.close();
			stmt.close();
			con.close();
			return users;

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public static void createUser(User user) {
		Connection con = null;
		PreparedStatement ps = null;

		try {
			con = DriverManager.getConnection("jdbc:mysql://localhost:3306/task_management_system?serverTimezone=UTC",
					"root", "password");

			String query = "INSERT INTO task_management_system.`user` (first_name, last_name, email, user_password, user_role) VALUES (?, ?, ?, ?, ?)";
			ps = con.prepareStatement(query);
			ps.setString(1, user.getFirstName());
			ps.setString(2, user.getLastName());
			ps.setString(3, user.getEmail());
			ps.setString(4, user.getPassword());
			ps.setString(5, user.getRole());

			int rowsAffected = ps.executeUpdate();
			System.out.println(rowsAffected + " row(s) created!");
			ps.close();
			con.close();

		} catch (SQLException e) {

			throw new RuntimeException(e);

		}
	}

	// To delete the user
	public static void deleteUser(int user_id) {
		try {
			Connection con = Connections.getConnection();
			PreparedStatement ps = con.prepareStatement("DELETE FROM task_management_system.`user` WHERE user_id=?");
			ps.setInt(1, user_id);
			int rs = ps.executeUpdate();
			ps.close();
			con.close();

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	// To get the user
	public static User getUserByID(long user_id) {
		User user = new User();
		try {
			java.sql.Connection con = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/task_management_system?serverTimezone=UTC", "root", "password");
			PreparedStatement ps = con.prepareStatement(
					"SELECT user_id, first_name, last_name, email, user_password, user_role FROM task_management_system.`user` WHERE user_id=?");
			ps.setLong(1, user_id);
			ResultSet rs = ps.executeQuery();
			if (rs.next()) {

				user.setFirstName(rs.getString("first_name"));
				user.setLastName(rs.getString("last_name"));
				user.setEmail(rs.getString("email"));

			}
			ps.close();
			con.close();

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return user;
	}

	// To update user Information
	public static boolean updateUser(User user, int id) {
		boolean isUpdate = false;
		try {
			java.sql.Connection con = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/task_management_system?serverTimezone=UTC", "root", "password");
			PreparedStatement ps = con.prepareStatement(
					"UPDATE task_management_system.`user` SET first_name=?, last_name=?, email=?, user_password=? WHERE user_id=?");
			ps.setString(1, user.getFirstName());
			ps.setString(2, user.getLastName());
			ps.setString(3, user.getEmail());
			ps.setString(4, user.getPassword());
			ps.setInt(5, id);

			int rs = ps.executeUpdate();

			if (rs > 0) {
				isUpdate = true;
			}
			ps.close();
			con.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return isUpdate;
	}

	// To check if the user exists when login
	public static long validateUser(String email, String password) {
		long userId = -1;
		Connection con = Connections.getConnection();

		try {
			Statement stmt = con.createStatement();
			String query = "SELECT user_id, first_name, last_name, email, user_password, user_role FROM task_management_system.`user` "
					+ "WHERE email='" + email + "' AND user_password='" + password + "'";
			ResultSet rs = stmt.executeQuery(query);

			if (rs.next()) {
				userId = rs.getLong("user_id");
			}

			rs.close();
			stmt.close();
			con.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

		return userId;
	}

	public static long getUserIdByEmail(String email) {
		long userId = 0;
		try {

			java.sql.Connection con = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/task_management_system?serverTimezone=UTC", "root", "password");
			PreparedStatement ps = con
					.prepareStatement("SELECT user_id FROM task_management_system.`user` WHERE email=?");
			ps.setString(1, email);
			ResultSet rs = ps.executeQuery();
			if (rs.next()) {
				userId = rs.getLong("user_id");
			}
			ps.close();
			con.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return userId;
	}

}
