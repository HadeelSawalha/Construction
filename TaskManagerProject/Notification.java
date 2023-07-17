package TaskManagerProject;

import javafx.application.Application;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Label;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class Notification extends BorderPane {

	GridPane n = new GridPane();
	List<Task> tasks = TaskDBHelper.showTasks(Constants.userID);

	public Notification(Stage primaryStage, Scene scene) {
		if (!TaskDBHelper.isResevedNotification(Constants.userID)) {
			tasks = TaskDBHelper.sendNotificationAndEmail(Constants.userID);
		}
		// Iterate through the tasks and create a label for each task
		int i = 0;
		for (Task task : tasks) {
			if (isDueToday(task)) {
				Label taskLabel = new Label(getTaskLabel(task));
				taskLabel.setStyle(Constants.labelStyle);
				taskLabel.setMinWidth(300); // Set the width of the label
				taskLabel.setMinHeight(30); // Set the height of the label
				n.add(taskLabel, 0, ++i);
			}

		}

		setCenter(n);
		n.setAlignment(Pos.TOP_CENTER);
		n.setVgap(15);
		n.setHgap(15);
	}

	// method to get the label text for a task
	private String getTaskLabel(Task task) {
		String label = "";
		label = "Task: " + task.getTaskName() + " | Due Date: " + task.getDueDate();
		label += " (Due Today)";

		return label;
	}

	public boolean isDueToday(Task task) {
		if (task.getDueDate().equals(LocalDate.now()))
			return true;

		return false;
	}

}
