package TaskManagerProject;

import java.time.LocalDate;
import java.util.ArrayList;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;

public class Constants {
	public static long userID = -1; // Variable to store the user ID
	public static long TaskID = 0; // Variable to store the task ID

	public static String tabStyle = "-fx-font-weight: bold; -fx-background-color: purple; -fx-font-size: 16px; -fx-padding: 7px 7px;";
	public static String textFieldStyle = "-fx-font-size: 16px;";
	public static String buttonStyle = "-fx-font-weight: bold; -fx-background-color: purple; -fx-font-size: 16px;";
	public static String labelStyle = "-fx-font-weight: bold; -fx-font-size: 16px;";

	public static ObservableList<Task> getDummyTasks() {
		// Create a dummy list of tasks for testing purposes
		ObservableList<Task> tasks = FXCollections.observableArrayList();
		tasks.add(new Task(1, "Task1", "Description 1", "Tag 1", LocalDate.now(), 1, new ArrayList<>(), 1, 1,
				Category.DESIGN + "", Status.OPEN + ""));
		tasks.add(new Task(2, "Task2", "Description 2", "Tag 2", LocalDate.parse("2023-06-15"), 5, new ArrayList<>(), 2,
				1, Category.BACK_END + "", Status.IN_PROGRESS + ""));
		tasks.add(new Task(3, "Task3", "Description 3", "Tag 3", LocalDate.parse("2023-06-10"), 2, new ArrayList<>(), 1,
				1, Category.FRONT_END + "", Status.VERIFIED + ""));
		tasks.add(new Task(4, "Task4", "Description 4", "Tag 4", LocalDate.now(), 5, new ArrayList<>(), 1, 1,
				Category.QA + "", Status.IN_PROGRESS + ""));
		return tasks;
	}
	
}
