package TaskManagerProject;

import utils.CollectionUtils;
import utils.MailUtils;

import java.sql.*;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;

public class TaskDBHelper {

	public static void createTask(long taskId, String taskName, String taskDescription, String tag, LocalDate dueDate,
			int priorityLevel, ArrayList<PreRequisite> preRequisites, long assignedUserId, long createdByUserId,
			String category, String status) {
		try {
			java.sql.Connection con = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/task_management_system?serverTimezone=UTC", "root", "password");

			Statement stmt = con.createStatement();
			String createTaskQuery = "INSERT INTO task_management_system.task (task_name, task_description, tag, priority_level, due_date, assigned_user_id, created_by_user_id, category, status) "
					+ "VALUES ('" + taskName + "', '" + taskDescription + "', '" + tag + "', " + priorityLevel + ", '"
					+ Date.valueOf(dueDate) + "', " + assignedUserId + ", " + createdByUserId + ", '" + category
					+ "', '" + status + "');";
			stmt.executeUpdate(createTaskQuery);
			stmt.close();
			con.close();

			if (CollectionUtils.isNotEmpty(preRequisites)) {
				con = DriverManager.getConnection(
						"jdbc:mysql://localhost:3306/task_management_system?serverTimezone=UTC", "root", "password");
				stmt = con.createStatement();
				String lastTaskIdQuery = "SELECT MAX(task_id) as last_id FROM task_management_system.task;";
				ResultSet rset = stmt.executeQuery(lastTaskIdQuery);
				stmt.close();
				con.close();

				if (rset.next()) {
					addPreRequisiteTask(rset.getLong("last_id"), preRequisites);
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public static ArrayList<Task> showTasks(long userId) {
		ArrayList<Task> tasks = new ArrayList<>();
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;

		try {
			con = DriverManager.getConnection("jdbc:mysql://localhost:3306/task_management_system?serverTimezone=UTC",
					"root", "password");
			String query = "SELECT * FROM task_management_system.task WHERE assigned_user_id=" + userId;
			stmt = con.createStatement();
			rs = stmt.executeQuery(query);

			while (rs.next()) {
				ArrayList<PreRequisite> pre_tasks = getListOfPreTask(rs.getLong("task_id"));
				Task task = new Task(rs.getLong("task_id"), rs.getString("task_name"), rs.getString("description"),
						rs.getString("tag"), rs.getDate("due_date").toLocalDate(), rs.getInt("priority_level"),
						pre_tasks, rs.getLong("assigned_user_id"), rs.getLong("created_by_user_id"),
						rs.getString("category"), rs.getString("status"));
				tasks.add(task);
			}
			rs.close();
			stmt.close();
			con.close();

		} catch (SQLException e) {
			throw new RuntimeException(e);

		}

		return tasks;
	}

	// TODO: rename (ALl) to getTasksFilteredByStatus
	public static ArrayList<Task> showTaskFilteredByStatus(long userId, String status) {
		ArrayList<Task> tasks = new ArrayList<>();
		try {
			java.sql.Connection con = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/task_management_system?serverTimezone=UTC", "root", "password");
			PreparedStatement ps = con.prepareStatement(
					"SELECT * FROM task_management_system.task where assigned_user_id =? AND status like ?");
			ps.setLong(1, userId);
			ps.setString(2, status);

			// TODO: Create common function to execute prepared statement and then convert
			// the result to a Task object
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				ArrayList<PreRequisite> pre_tasks = getListOfPreTask(rs.getLong("task_id"));
				System.out.println(pre_tasks.toString());
				Task task = new Task(rs.getLong("task_id"), rs.getString("task_name"), rs.getString("description"),
						rs.getString("tag"), rs.getDate("due_date").toLocalDate(), rs.getInt("priority_level"),
						pre_tasks, rs.getLong("assigned_user_id"), rs.getLong("created_by_user_id"),
						rs.getString("category"), rs.getString("status"));

				tasks.add(task);
			}
			rs.close();
			ps.close();
			con.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return tasks;

	}

	// TODO: rename to getTasksSortedByPriority
	public static ArrayList<Task> showTaskFilteredByPriority(long userId) {
		ArrayList<Task> tasks = new ArrayList<>();
		try {
			java.sql.Connection con = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/task_management_system?serverTimezone=UTC", "root", "password");
			PreparedStatement ps = con.prepareStatement(
					"SELECT * FROM task_management_system.task where assigned_user_id=? ORDER BY priority_level DESC");
			ps.setLong(1, userId);
			ResultSet rs = ps.executeQuery();

			while (rs.next()) {
				ArrayList<PreRequisite> pre_tasks = getListOfPreTask(rs.getLong("task_id"));
				Task task = new Task(rs.getLong("task_id"), rs.getString("task_name"), rs.getString("description"),
						rs.getString("tag"), rs.getDate("due_date").toLocalDate(), rs.getInt("priority_level"),
						pre_tasks, rs.getLong("assigned_user_id"), rs.getLong("created_by_user_id"),
						rs.getString("category"), rs.getString("status"));
				tasks.add(task);
			}
			rs.close();
			ps.close();
			con.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return tasks;
	}

	// TODO: rename to getTasksSortedByDeadline
	// TODO: Create order enum (DESC, ASC)
	public static ArrayList<Task> showTaskFilteredByDeadline(long userId, String order) {
		ArrayList<Task> tasks = new ArrayList<>();
		try {
			java.sql.Connection con = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/task_management_system?serverTimezone=UTC", "root", "password");
			PreparedStatement ps = con.prepareStatement(
					"SELECT * FROM task_management_system.task where assigned_user_id=? ORDER BY due_date ?");
			ps.setLong(1, userId);
			ps.setString(2, order);
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				ArrayList<PreRequisite> pre_tasks = getListOfPreTask(rs.getLong("task_id"));
				Task task = new Task(rs.getLong("task_id"), rs.getString("task_name"), rs.getString("description"),
						rs.getString("tag"), rs.getDate("due_date").toLocalDate(), rs.getInt("priority_level"),
						pre_tasks, rs.getLong("assigned_user_id"), rs.getLong("created_by_user_id"),
						rs.getString("category"), rs.getString("status"));
				tasks.add(task);
			}
			rs.close();
			ps.close();
			con.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return tasks;

	}

	// TODO: rename to getListOfPreTaskIds
	public static ArrayList<PreRequisite> getListOfPreTask(long taskId) {
		ArrayList<Integer> listOfPreRequestId = new ArrayList<>();
		ArrayList<PreRequisite> pre_task = new ArrayList<>();
		PreRequisite task = null;
		PreparedStatement ps;
		java.sql.Connection con;
		try {
			con = DriverManager.getConnection("jdbc:mysql://localhost:3306/task_management_system?serverTimezone=UTC",
					"root", "password");
			ps = con.prepareStatement(
					"SELECT pre_request_task_id FROM task_management_system.pre_request_task where task_id=?");
			ps.setLong(1, taskId);
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				listOfPreRequestId.add(rs.getInt("pre_request_task_id"));
			}
			rs.close();
			ps.close();

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		if (listOfPreRequestId.size() > 0) {
			try {
				String sql = "SELECT task_id, task_name FROM task_management_system.task where task_id IN (";
				for (int i = 0; i < listOfPreRequestId.size(); i++) {
					sql += (i == 0 ? "?" : ", ?");
				}
				sql += ")";
				ps = con.prepareStatement(sql);
				for (int i = 0; i < listOfPreRequestId.size(); i++) {
					ps.setInt(i + 1, listOfPreRequestId.get(i));
				}

				ResultSet rs = ps.executeQuery();
				while (rs.next()) {
					task = new PreRequisite(rs.getLong("task_id"), rs.getString("task_name"));
					pre_task.add(task);
				}
				rs.close();
				ps.close();
				con.close();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		} else {
			try {
				con.close();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}

		return pre_task;
	}

	public static void updateTaskName(long taskId, String taskName) {
		try (java.sql.Connection con = DriverManager.getConnection(
				"jdbc:mysql://localhost:3306/task_management_system?serverTimezone=UTC", "root", "password");
				PreparedStatement ps = con.prepareStatement(Queries.UPDATE_TASK_SQLQUERY);) {
			ps.setString(1, taskName);
			ps.setLong(2, taskId);
			ps.executeUpdate();
			ps.close();
			con.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public static void updateTaskDescription(long taskId, String taskDescription) {

		try {
			java.sql.Connection con = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/task_management_system?serverTimezone=UTC", "root", "password");
			PreparedStatement ps = con
					.prepareStatement("UPDATE task_management_system.task SET description=? WHERE task_id=?");
			ps.setString(1, taskDescription);
			ps.setLong(2, taskId);
			ps.executeUpdate();
			ps.close();
			con.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public static void updateTaskStatus(long taskId, String taskStatus) {
		try {
			java.sql.Connection con = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/task_management_system?serverTimezone=UTC", "root", "password");
			PreparedStatement ps = con
					.prepareStatement("UPDATE task_management_system.task SET status=? WHERE task_id=?");
			ps.setString(1, taskStatus);
			ps.setLong(2, taskId);
			ps.executeUpdate();
			ps.close();
			con.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public static void updateTaskCategory(long taskId, String taskCategory) {
		try {
			Category.valueOf(taskCategory);
		} catch (Exception e) {
			throw new RuntimeException("Please provide valid category!");
		}

		try {
			java.sql.Connection con = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/task_management_system?serverTimezone=UTC", "root", "password");
			PreparedStatement ps = con
					.prepareStatement("UPDATE task_management_system.task SET category=? WHERE task_id=?");
			ps.setString(1, taskCategory);
			ps.setLong(2, taskId);
			ps.executeUpdate();
			ps.close();
			con.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public static void updateTaskPriorityLevel(long taskId, int priorityLevel) {
		try {
			java.sql.Connection con = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/task_management_system?serverTimezone=UTC", "root", "password");
			PreparedStatement ps = con
					.prepareStatement("UPDATE task_management_system.task SET priority_level=? WHERE task_id=?");
			ps.setInt(1, priorityLevel);
			ps.setLong(2, taskId);
			ps.executeUpdate();
			ps.close();
			con.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public static void updateTaskDeadline(long taskId, LocalDate deadline) {
		try {
			java.sql.Connection con = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/task_management_system?serverTimezone=UTC", "root", "password");
			PreparedStatement ps = con
					.prepareStatement("UPDATE task_management_system.task SET due_date=? WHERE task_id=?");
			ps.setDate(1, Date.valueOf(deadline));
			ps.setLong(2, taskId);
			ps.executeUpdate();
			ps.close();
			con.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public static void updatePreRequisiteTask(long taskId, ArrayList<PreRequisite> preRequisites) {
		try {
			java.sql.Connection con = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/task_management_system?serverTimezone=UTC", "root", "password");
			PreparedStatement ps = con.prepareStatement(
					"SELECT  pre_request_task_id FROM task_management_system.pre_request_task where task_id=?");
			ps.setLong(1, taskId);
			ResultSet rs = ps.executeQuery();
			if (rs.next()) {
				deletePreRequisiteTask(taskId);
			}
			addPreRequisiteTask(taskId, preRequisites);
			ps.close();
			con.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public static void addPreRequisiteTask(long taskId, ArrayList<PreRequisite> preRequisites) {
		for (PreRequisite task : preRequisites) {
			try {
				java.sql.Connection con = DriverManager.getConnection(
						"jdbc:mysql://localhost:3306/task_management_system?serverTimezone=UTC", "root", "password");
				PreparedStatement ps = con.prepareStatement(
						"INSERT INTO task_management_system.pre_request_task (task_id, pre_request_task_id) VALUES(?, ?)");
				ps.setLong(1, taskId);
				ps.setLong(2, task.getPreTaskId());
				System.out.println(ps.executeUpdate());
				ps.close();
				con.close();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}

	}

	public static void deletePreRequisiteTask(long taskId) {
		try {
			java.sql.Connection con = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/task_management_system?serverTimezone=UTC", "root", "password");
			PreparedStatement ps = con
					.prepareStatement("DELETE FROM task_management_system.pre_request_task WHERE task_id=?");
			ps.setLong(1, taskId);
			ps.executeUpdate();
			ps.close();
			con.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

	}

	public static void updateAssignedUser(long taskId, long assignedUserId) {

		try {
			java.sql.Connection con = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/task_management_system?serverTimezone=UTC", "root", "password");
			PreparedStatement ps = con
					.prepareStatement("UPDATE task_management_system.task SET assigned_user_id=? WHERE task_id=?");
			ps.setLong(1, assignedUserId);
			ps.setLong(2, taskId);
			ps.executeUpdate();
			ps.close();
			con.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	// TODO: use soft delete instead of hard delete
	public static void deleteTask(Long taskId) {
		try {
			java.sql.Connection con = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/task_management_system?serverTimezone=UTC", "root", "password");
			PreparedStatement ps = con.prepareStatement("DELETE FROM task_management_system.task WHERE task_id=?");
			ps.setLong(1, taskId);
			ps.executeUpdate();
			ps.close();
			con.close();

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public static boolean isResevedNotification(long user_id) {
		boolean isResevedNotification = false;
		LocalDate todayDate = LocalDate.now();

		try {
			java.sql.Connection con = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/task_management_system?serverTimezone=UTC", "root", "password");
			PreparedStatement ps = con.prepareStatement(
					"SELECT notification_id, send, `date`, user_id FROM task_management_system.notification WHERE user_id=? and date=? ");
			ps.setLong(1, user_id);
			ps.setDate(2, Date.valueOf(todayDate));
			ResultSet rs = ps.executeQuery();
			if (rs.next()) {
				isResevedNotification = true;
			}

			rs.close();
			ps.close();

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return isResevedNotification;
	}

	public static ArrayList<Task> sendNotificationAndEmail(long user_id) {
		ArrayList<Task> tasks = new ArrayList<>();
		LocalDate todayDate = LocalDate.now();
		LocalDate nextdate = todayDate.plusDays(1);
		User user = UserDbHelper.getUserByID(user_id);
		java.sql.Connection con;
		try {
			con = DriverManager.getConnection("jdbc:mysql://localhost:3306/task_management_system?serverTimezone=UTC",
					"root", "password");
			PreparedStatement ps = con.prepareStatement(
					"SELECT * FROM task_management_system.task where assigned_user_id=? and due_date=? or due_date=? ");
			ps.setLong(1, user_id);
			ps.setDate(2, Date.valueOf(todayDate));
			ps.setDate(3, Date.valueOf(nextdate));
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				ArrayList<PreRequisite> pre_tasks = getListOfPreTask(rs.getLong("task_id"));
				Task task = new Task(rs.getLong("task_id"), rs.getString("task_name"), rs.getString("description"),
						rs.getString("tag"), rs.getDate("due_date").toLocalDate(), rs.getInt("priority_level"),
						pre_tasks, rs.getLong("assigned_user_id"), rs.getLong("created_by_user_id"),
						rs.getString("category"), rs.getString("status"));
				tasks.add(task);

			}

			rs.close();
			ps.close();

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		if (!isResevedNotification(user_id)) {
			MailUtils.sendEmail(user.getEmail(), "Tasks to be delivered today and tomorrow",
					MailUtils.buildTaskAssignedBody(user.getFirstName(), tasks));
		}
		try {
			PreparedStatement ps = con.prepareStatement(
					"INSERT INTO task_management_system.notification (send, `date`, user_id) VALUES(?, ?, ?) ");
			ps.setBoolean(1, true);
			ps.setDate(2, Date.valueOf(todayDate));
			ps.setLong(3, user_id);
			ps.executeUpdate();
			ps.close();
			con.close();

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

		return tasks;

	}

	public static long getTaskIdByTaskName(String taskName) {
		long taskId = 0;
		try {
			java.sql.Connection con = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/task_management_system?serverTimezone=UTC", "root", "password");
			PreparedStatement ps = con
					.prepareStatement("SELECT task_id FROM task_management_system.task WHERE task_name=?");
			ps.setString(1, taskName);
			ResultSet rs = ps.executeQuery();
			if (rs.next()) {
				taskId = rs.getLong("task_id");
			}
			ps.close();
			con.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return taskId;
	}

}
