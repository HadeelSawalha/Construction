package TaskManagerProject;

import javafx.application.Application;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.Scene;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.layout.BorderPane;
import javafx.stage.Stage;

import java.util.ArrayList;

public class UserView extends BorderPane {

	private TableView<User> userTable;

	public UserView(Stage primaryStage, Scene scene) {

		// Create the user table
		userTable = new TableView<>();
		TableColumn<User, String> firstNameCol = new TableColumn<>("firstName");
		firstNameCol.setCellValueFactory(new PropertyValueFactory<>("firstName"));
		firstNameCol.setPrefWidth(200);

		TableColumn<User, String> lastNameCol = new TableColumn<>("lastName");
		lastNameCol.setCellValueFactory(new PropertyValueFactory<>("lastName"));
		lastNameCol.setPrefWidth(200);

		TableColumn<User, String> emailCol = new TableColumn<>("Email");
		emailCol.setCellValueFactory(new PropertyValueFactory<>("email"));
		emailCol.setPrefWidth(200);

		userTable.getColumns().addAll(firstNameCol, lastNameCol, emailCol);

		// Set the user data
		ObservableList<User> users = FXCollections.observableArrayList(showUsers());
		userTable.setItems(users);

		setCenter(userTable);

		primaryStage.setScene(scene);
		primaryStage.setTitle("User Management");
		primaryStage.show();
	}

	public static ArrayList<User> showUsers() {
		ArrayList<User> users = new ArrayList<>();
		users = UserDbHelper.showUsers();
		return users;
	}

//	fot test :
//	public static ArrayList<User> showUsers() {
//		ArrayList<User> users = new ArrayList<>();
//		users.add(new User("hadeel", "Sawalha", "pass1", "hadeel@gmail.com", "Admin"));
//		users.add(new User("Hiba", "Naser", "pass1", "hiba@gmail.com", "User"));
//		users.add(new User("Majed", "Ayyad", "pass1", "majed@gmail.com", "User"));
//		return users;
//	}

}
