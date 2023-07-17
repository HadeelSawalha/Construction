package TaskManagerProject;

import javafx.beans.property.*;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.application.Application;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.ColumnConstraints;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.GridPane;
import javafx.scene.paint.Color;
import javafx.stage.Modality;
import javafx.stage.Stage;

import java.time.LocalDate;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.GridPane;
import javafx.scene.paint.Color;
import javafx.stage.Modality;
import javafx.stage.Stage;

public class TaskView extends BorderPane {

	private TableView<Task> taskTable;
	private TextField searchField;
	public long TaskId;
	private ComboBox<Integer> priorityFilterComboBox;
	private DatePicker deadlineFilterDatePicker;
	private ComboBox<String> userFilterComboBox;
	private ComboBox<String> statusFilterComboBox;

	public TaskView(Stage primaryStage, Scene scene) {
		// Create the UI elements
		GridPane gp = new GridPane();
		HBox topBox = new HBox();
		VBox centerBox = new VBox();
		Label titleLabel = new Label("Task Manager Dashboard");

			// Create the task table
			TableColumn<Task, Long> idCol = new TableColumn<>("ID");
			idCol.setCellValueFactory(new PropertyValueFactory<>("taskId"));

			TableColumn<Task, String> nameCol = new TableColumn<>("Name");
			nameCol.setCellValueFactory(new PropertyValueFactory<>("taskName"));

			TableColumn<Task, String> tagCol = new TableColumn<>("Tag");
			tagCol.setCellValueFactory(new PropertyValueFactory<>("tag"));

			TableColumn<Task, String> descCol = new TableColumn<>("Description");
			descCol.setCellValueFactory(new PropertyValueFactory<>("taskDescription"));

			TableColumn<Task, LocalDate> dueDateCol = new TableColumn<>("Due Date");
			dueDateCol.setCellValueFactory(new PropertyValueFactory<>("dueDate"));

			TableColumn<Task, Integer> priorityCol = new TableColumn<>("Priority");
			priorityCol.setCellValueFactory(new PropertyValueFactory<>("priorityLevel"));

			TableColumn<Task, Status> statusCol = new TableColumn<>("Status");
			statusCol.setCellValueFactory(new PropertyValueFactory<>("status"));

			TableColumn<Task, List<Task>> prereqCol = new TableColumn<>("Prerequisite");
			prereqCol.setCellValueFactory(new PropertyValueFactory<>("preRequisiteTasks"));

			TableColumn<Task, Long> assignedUserCol = new TableColumn<>("AssignUser");
			assignedUserCol.setCellValueFactory(new PropertyValueFactory<>("assignedUserId"));

			TableColumn<Task, Long> createdByUserCol = new TableColumn<>("Created By User ID");
			createdByUserCol.setCellValueFactory(new PropertyValueFactory<>("createdByUserId"));

			TableColumn<Task, Category> categoryCol = new TableColumn<>("Category");
			categoryCol.setCellValueFactory(new PropertyValueFactory<>("category"));

			TableColumn<Task, Void> deleteCol = new TableColumn<>("Delete");
			deleteCol.setCellFactory(param -> new TableCell<>() {
				private final Button deleteButton = new Button("Delete");

				{
					deleteButton.setOnAction(event -> {
						Task task = getTableView().getItems().get(getIndex());
						deleteTask(task);
					});
				}

				@Override
				protected void updateItem(Void item, boolean empty) {
					super.updateItem(item, empty);
					if (empty) {
						setGraphic(null);
					} else {
						setGraphic(deleteButton);
					}
				}
			});

			TableColumn<Task, Void> editCol = new TableColumn<>("Edit");
			editCol.setCellFactory(param -> new TableCell<>() {
				private final Button editButton = new Button("Edit");

				{

					editButton.setOnAction(event -> {
						Task task = getTableView().getItems().get(getIndex());
						editTask(task);
					});
				}

				@Override
				protected void updateItem(Void item, boolean empty) {
					super.updateItem(item, empty);
					if (empty) {
						setGraphic(null);
					} else {
						setGraphic(editButton);
					}
				}
			});

			taskTable = new TableView<>();
			taskTable.getColumns().addAll(idCol, nameCol, tagCol, descCol, dueDateCol, priorityCol, statusCol,
					prereqCol, assignedUserCol, categoryCol, deleteCol, editCol);
			taskTable.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);
			//forTest : taskTable.setItems(Constants.getDummyTasks());
			 taskTable.setItems(getTasksForUser(Constants.userID));
			 
			// Create the menu items for each filter
			MenuItem priorityMenuItem = new MenuItem("Priority");
			priorityFilterComboBox = new ComboBox<>();
			priorityFilterComboBox.getItems().addAll(1, 2, 3, 4, 5);
			priorityMenuItem.setGraphic(priorityFilterComboBox);

			MenuItem deadlineMenuItem = new MenuItem("Deadline");
			deadlineFilterDatePicker = new DatePicker();
			deadlineMenuItem.setGraphic(deadlineFilterDatePicker);

			MenuItem userMenuItem = new MenuItem("User");
			userFilterComboBox = new ComboBox<>();
			userFilterComboBox.getItems().addAll("User 1", "User 2", "User 3");
			userMenuItem.setGraphic(userFilterComboBox);

			MenuItem statusMenuItem = new MenuItem("Status");
			statusFilterComboBox = new ComboBox<>();
			statusFilterComboBox.getItems().addAll("OPEN", "IN_PROGRESS", "VERIFIED", "COMPLETED");
			statusMenuItem.setGraphic(statusFilterComboBox);

			centerBox.getChildren().addAll(taskTable);
			centerBox.setSpacing(10);
			centerBox.setPadding(new Insets(10));

			// Add the UI elements to the root layout
			setTop(topBox);
			setCenter(centerBox);

			gp.add(taskTable, 0, 1);
			setCenter(gp);
			gp.setAlignment(Pos.CENTER);
			gp.setVgap(15);
			gp.setHgap(15);

			// Set custom colors
			topBox.setStyle("-fx-background-color: green; -fx-text-fill: white;");
			centerBox.setStyle("-fx-background-color: #ffffff;");

			  // Set the column constraints for the GridPane
            ColumnConstraints columnConstraints = new ColumnConstraints();
            columnConstraints.setPercentWidth(200);
            gp.getColumnConstraints().add(columnConstraints);
			
			primaryStage.setScene(scene);
			primaryStage.setTitle("Task Manager Dashboard");
			primaryStage.show();
	}


    private ObservableList<Task> getTasksForUser(long userId) {
        ArrayList<Task> tasks = TaskDBHelper.showTasks(userId);
        return FXCollections.observableArrayList(tasks);
    }

	private void deleteTask(Task task) {
		taskTable.getItems().remove(task);
		TaskDBHelper.deleteTask(task.getTaskId());
	}

	private void editTask(Task task) {
		// Open the edit task dialog and pass the task object
		EditTaskDialog dialog = new EditTaskDialog(task);
		Optional<Task> result = dialog.showAndWait();

		result.ifPresent(updatedTask -> {
			int index = taskTable.getItems().indexOf(task);
			taskTable.getItems().set(index, updatedTask);
		});
	}

}
