package TaskManagerProject;

import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.paint.Color;
import javafx.scene.text.Font;
import javafx.stage.Screen;
import javafx.stage.Stage;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class CreateTask extends BorderPane {
	private GridPane gridPane = new GridPane();

	public CreateTask(Stage primaryStage, Scene scene) {
		// Name field
		Label nameLabel = new Label("Name:");
		nameLabel.setStyle(Constants.labelStyle);
		TextField nameTextField = new TextField();
		nameTextField.setStyle(Constants.textFieldStyle);
		gridPane.add(nameLabel, 0, 0);
		gridPane.add(nameTextField, 1, 0);

		// Description field
		Label descLabel = new Label("Description:");
		descLabel.setStyle(Constants.labelStyle);
		TextField descTextField = new TextField();
		descTextField.setStyle(Constants.textFieldStyle);
		gridPane.add(descLabel, 0, 1);
		gridPane.add(descTextField, 1, 1);

		// Tag field
		Label tagLabel = new Label("Tag:");
		tagLabel.setStyle(Constants.labelStyle);
		TextField tagTextField = new TextField();
		tagTextField.setStyle(Constants.textFieldStyle);
		gridPane.add(tagLabel, 0, 2);
		gridPane.add(tagTextField, 1, 2);

		// Due Date field
		Label dueDateLabel = new Label("Due Date:");
		dueDateLabel.setStyle(Constants.labelStyle);
		DatePicker dueDatePicker = new DatePicker();
		dueDatePicker.setStyle(Constants.textFieldStyle);
		gridPane.add(dueDateLabel, 0, 3);
		gridPane.add(dueDatePicker, 1, 3);

		// Priority Level field
		Label priorityLabel = new Label("Priority Level:");
		priorityLabel.setStyle(Constants.labelStyle);
		ComboBox<Integer> priorityComboBox = new ComboBox<>();
		priorityComboBox.getItems().addAll(1, 2, 3, 4, 5);
		priorityComboBox.setValue(1);
		priorityComboBox.setStyle(Constants.textFieldStyle);
		gridPane.add(priorityLabel, 0, 4);
		gridPane.add(priorityComboBox, 1, 4);

		// Prerequisites field
		Label prereqLabel = new Label("Prerequisites:");
		prereqLabel.setStyle(Constants.labelStyle);
		ComboBox<String> preReqComboBox = new ComboBox<>();
		ArrayList<Task> tasks = TaskDBHelper.showTasks(Constants.userID);
		for (int i = 0; i < tasks.size(); i++) {
			preReqComboBox.getItems().add(tasks.get(i).getTaskName());
		}
		//for test : preReqComboBox.getItems().addAll("Task 1", "Task 2", "Task 3", "Task 4");
		preReqComboBox.setStyle(Constants.textFieldStyle);
		gridPane.add(prereqLabel, 0, 5);
		gridPane.add(preReqComboBox, 1, 5);

		// Assigned User ID field
		Label assignedUserLabel = new Label("Assigned User:");
		assignedUserLabel.setStyle(Constants.labelStyle);
		ComboBox<String> assignedComboBox = new ComboBox<>();
		ArrayList<User> users = UserDbHelper.showUsers();
		for (int i = 0; i < users.size(); i++) {
			assignedComboBox.getItems().add(users.get(i).getEmail());
		}
		//for test : assignedComboBox.getItems().addAll("Hadeel@gmail.com", "Hiba@gmail.com", "Majed@gmail.com");
		assignedComboBox.setStyle(Constants.textFieldStyle);
		gridPane.add(assignedUserLabel, 0, 6);
		gridPane.add(assignedComboBox, 1, 6);

		// Status field
		Label statusLabel = new Label("Status:");
		statusLabel.setStyle(Constants.labelStyle);
		ComboBox<String> statusComboBox = new ComboBox<>();
		statusComboBox.getItems().addAll("OPEN", "IN_PROGRESS", "VERIFIED", "COMPLETED", "INCOMPLETE");
		statusComboBox.setValue("OPEN");
		statusComboBox.setStyle(Constants.textFieldStyle);
		gridPane.add(statusLabel, 0, 8);
		gridPane.add(statusComboBox, 1, 8);

		// Category field
		Label categoryLabel = new Label("Category:");
		categoryLabel.setStyle(Constants.labelStyle);
		ComboBox<String> categoryComboBox = new ComboBox<>();
		categoryComboBox.getItems().addAll("FRONT_END", "BACK_END", "DESIGN", "QA");
		categoryComboBox.setValue("FRONT_END");
		categoryComboBox.setStyle(Constants.textFieldStyle);
		gridPane.add(categoryLabel, 0, 9);
		gridPane.add(categoryComboBox, 1, 9);

		Button createButton = new Button("Create");
		createButton.setStyle(Constants.buttonStyle);

		HBox buttonBox = new HBox(10, createButton);
		buttonBox.setAlignment(Pos.CENTER);
		gridPane.add(buttonBox, 1, 10);

		createButton.setOnAction(event -> {
			String name = nameTextField.getText();
			String description = descTextField.getText();
			String tag = tagTextField.getText();
			LocalDate dueDate = dueDatePicker.getValue();
			int priority = priorityComboBox.getValue();

			long preReqID = TaskDBHelper.getTaskIdByTaskName(preReqComboBox.getValue());
			ArrayList<PreRequisite> prerequisites = new ArrayList<PreRequisite>();
			prerequisites.add(new PreRequisite(preReqID, preReqComboBox.getValue()));

			long assignedUserID = UserDbHelper.getUserIdByEmail(assignedComboBox.getValue());
			Status status = Status.valueOf(statusComboBox.getValue());
			Category category = Category.valueOf(categoryComboBox.getValue());

			TaskDBHelper.createTask(++Constants.TaskID, name, description, tag, dueDate, priority, prerequisites,
						assignedUserID, Constants.userID, category + "", status + "");
			
 			
//			Task task = new Task(++Constants.TaskID, name, description, tag, dueDate, priority, prerequisites,
//				assignedUserID, Constants.userID, category + "", status + "");
//			TaskDBHelper.createTask(task);

			// for test : System.out.println("create task--->"+ 1+ name+ description+ tag+ dueDate+
			// priority+ prerequisites+ assignedUserID+ 1+ category + ""+ status + "");
		});

		setCenter(gridPane);
		gridPane.setAlignment(Pos.CENTER);
		gridPane.setVgap(15);
		gridPane.setHgap(15);
		gridPane.setPadding(new Insets(10));
	}

}
