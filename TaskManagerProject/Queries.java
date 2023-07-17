package TaskManagerProject;

public interface Queries {
   String CREATE_TASK_SQL_QUERY = "INSERT INTO task_management_system.task (task_name, description, tag, " +
            "priority_level, due_date, assigned_user_id, created_by_user_id, category, stutes)" +
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)";

}
