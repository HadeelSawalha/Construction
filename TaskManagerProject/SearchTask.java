package TaskManagerProject;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import javafx.application.Application;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.stage.Stage;

public class SearchTask extends BorderPane {
	private TableView<Task> tableView;
	private TextField searchTextField;

	public SearchTask(Stage primaryStage, Scene scene) {
		GridPane gridPane = new GridPane();
		gridPane.setVgap(15);
		gridPane.setHgap(15);
		gridPane.setAlignment(Pos.CENTER);
		gridPane.setPadding(new Insets(15));

		Label searchLabel = new Label("Search:");
		searchTextField = new TextField();

		Button searchButton = new Button("Search");

		gridPane.add(searchLabel, 0, 0);
		gridPane.add(searchTextField, 1, 0);
		gridPane.add(searchButton, 2, 0);

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

		searchTextField.setPromptText("Search tasks...");
		searchButton.setOnAction(event -> {
			String searchText = searchTextField.getText().toLowerCase();
			ObservableList<Task> filteredTasks = searchTasks(searchTasks(), searchText);
			tableView.setItems(filteredTasks);
		});
	}

	public ObservableList<Task> searchTasks(ArrayList<Task> tasks, String searchText) {
		ObservableList<Task> filteredList = FXCollections.observableArrayList();

		for (Task task : tasks) {
			if (searchText.isEmpty() || task.getTaskName().toLowerCase().contains(searchText)
					|| task.getTaskDescription().toLowerCase().contains(searchText)
					|| (task.getStatus() + "").toLowerCase().contains(searchText)
					|| (task.getCategory() + "").toLowerCase().contains(searchText)) {
				filteredList.add(task);
			}
		}

		return filteredList;
	}

	private ArrayList<Task> searchTasks() {
		return TaskDBHelper.showTasks(Constants.userID);
	}
	
	//for test:	
	//	private ObservableList<Task> searchTasks() {
	//		return Constants.getDummyTasks();
	//	}

}
