package TaskManagerProject;

import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.control.cell.CheckBoxTableCell;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.GridPane;
import javafx.stage.Stage;
import javafx.util.Callback;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class FilterTask extends BorderPane {
	private TableView<Task> tableView;
	private ComboBox<String> priorityComboBox;
	private ComboBox<LocalDate> deadlineComboBox;
	private ComboBox<String> userComboBox;
	private ComboBox<String> statusComboBox;

	public FilterTask(Stage primaryStage, Scene scene) {
		GridPane gridPane = new GridPane();
		gridPane.setVgap(15);
		gridPane.setHgap(15);
		gridPane.setAlignment(Pos.CENTER);
		gridPane.setPadding(new Insets(15));

		Label priorityLabel = new Label("Priority:");
		priorityComboBox = new ComboBox<>();
		priorityComboBox.getItems().addAll("1", "2", "3", "4", "5");

		Label deadlineLabel = new Label("Deadline:");
		deadlineComboBox = new ComboBox<>();
		ArrayList<Task> tasks = TaskDBHelper.showTasks(Constants.userID);
		for (int i = 1; i < tasks.size(); i++) {
			deadlineComboBox.getItems().add(tasks.get(i).getDueDate());
		}

		// for test : deadlineComboBox.getItems().addAll(LocalDate.parse("2023-05-31"),LocalDate.parse("2023-06-15"),LocalDate.parse("2023-06-10"));

		Label userLabel = new Label("User:");
		userComboBox = new ComboBox<>();
		ArrayList<User> users = UserDbHelper.showUsers();
		for (int i = 0; i < users.size(); i++) {
			userComboBox.getItems().add(users.get(i).getEmail());
		}
		// for test : userComboBox.getItems().addAll("Hadeel@gmail.com","Hiba@gmail.com", "Majed@gmail.com");

		Label statusLabel = new Label("Status:");
		statusComboBox = new ComboBox<>();
		statusComboBox.getItems().addAll("OPEN", "IN_PROGRESS", "VERIFIED", "COMPLETED", "INCOMPLETE");

		Button filterButton = new Button("Filter");

		gridPane.add(priorityLabel, 0, 0);
		gridPane.add(priorityComboBox, 1, 0);
		gridPane.add(deadlineLabel, 2, 0);
		gridPane.add(deadlineComboBox, 3, 0);
		gridPane.add(userLabel, 4, 0);
		gridPane.add(userComboBox, 5, 0);
		gridPane.add(statusLabel, 6, 0);
		gridPane.add(statusComboBox, 7, 0);
		gridPane.add(filterButton, 8, 0);

		tableView = new TableView<>();
		TableColumn<Task, Long> idCol = new TableColumn<>("ID");
		idCol.setCellValueFactory(new PropertyValueFactory<>("taskId"));
		idCol.setPrefWidth(100);

		TableColumn<Task, String> nameCol = new TableColumn<>("Name");
		nameCol.setCellValueFactory(new PropertyValueFactory<>("taskName"));
		nameCol.setPrefWidth(100);

		TableColumn<Task, String> tagCol = new TableColumn<>("Tag");
		tagCol.setCellValueFactory(new PropertyValueFactory<>("tag"));
		tagCol.setPrefWidth(100);

		TableColumn<Task, String> descCol = new TableColumn<>("Description");
		descCol.setCellValueFactory(new PropertyValueFactory<>("taskDescription"));
		descCol.setPrefWidth(100);

		TableColumn<Task, Integer> priorityCol = new TableColumn<>("Priority");
		priorityCol.setCellValueFactory(new PropertyValueFactory<>("priorityLevel"));
		priorityCol.setPrefWidth(100);

		TableColumn<Task, List<Task>> prereqCol = new TableColumn<>("Prerequisites");
		prereqCol.setCellValueFactory(new PropertyValueFactory<>("preRequisites"));
		prereqCol.setPrefWidth(150);

		TableColumn<Task, Status> statusCol = new TableColumn<>("Status");
		statusCol.setCellValueFactory(new PropertyValueFactory<>("status"));
		statusCol.setPrefWidth(100);

		TableColumn<Task, Long> assignedUserCol = new TableColumn<>("AssignedUser");
		assignedUserCol.setCellValueFactory(new PropertyValueFactory<>("assignedUserId"));
		assignedUserCol.setPrefWidth(150);

		TableColumn<Task, LocalDate> dueDateCol = new TableColumn<>("Due Date");
		dueDateCol.setCellValueFactory(new PropertyValueFactory<>("dueDate"));
		dueDateCol.setPrefWidth(100);

		TableColumn<Task, Category> categoryCol = new TableColumn<>("Category");
		categoryCol.setCellValueFactory(new PropertyValueFactory<>("category"));
		categoryCol.setPrefWidth(100);

		tableView.getColumns().addAll(idCol, nameCol, tagCol, descCol, priorityCol, prereqCol, statusCol,
				assignedUserCol, dueDateCol, categoryCol);

		setTop(gridPane);
		setCenter(tableView);

		filterButton.setOnAction(e -> filterTasks());
	}



    private void filterTasks() {
        String priority = priorityComboBox.getValue();
        LocalDate deadline = deadlineComboBox.getValue();
        String status = statusComboBox.getValue();

        ObservableList<Task> filteredTasks = FXCollections.observableArrayList();

        if (priority != null) {
            ArrayList<Task> priorityFilteredTasks =TaskDBHelper.showTaskFilteredByPriority(Constants.userID);
            filteredTasks.addAll(priorityFilteredTasks);
        }
        
        if (deadline != null) {
            ArrayList<Task> deadlineFilteredTasks = TaskDBHelper.showTaskFilteredByDeadline(Constants.userID, "ASC");
            filteredTasks.addAll(deadlineFilteredTasks);
        }

        if (status != null) {
            ArrayList<Task> statusFilteredTasks =TaskDBHelper. showTaskFilteredByStatus(Constants.userID, status);
            filteredTasks.addAll(statusFilteredTasks);
        }

        tableView.setItems(filteredTasks);
    }

// for test
//    private void filterTasks() {
//		String priority = priorityComboBox.getValue();
//		LocalDate deadline = deadlineComboBox.getValue();
//		String user = userComboBox.getValue();
//		String status = statusComboBox.getValue();
//
//		ObservableList<Task> filteredTasks = FXCollections.observableArrayList();
//
//		for (Task task : Constants.getDummyTasks()) {
//			if ((priority == null || (task.getPriorityLevel() + "").equals(priority))
//					&& (deadline == null || task.getDueDate().equals(deadline))
//					&& (user == null || (task.getAssignedUserId() + "").equals(user))
//					&& (status == null || task.getStatus().equals(status))) {
//				filteredTasks.add(task);
//			}
//		}
//
//		tableView.setItems(filteredTasks);
//	}
}



