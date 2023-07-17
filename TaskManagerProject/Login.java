package TaskManagerProject;

import javafx.application.Application;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.layout.Background;
import javafx.scene.layout.BackgroundFill;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.GridPane;
import javafx.scene.paint.Color;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;
import javafx.scene.text.Text;
import javafx.stage.Stage;

public class Login extends Application {
	private Stage stage;

	@Override
	public void start(Stage stage) {
		this.stage = stage;
		stage.setTitle("Task Manager Login");

		BorderPane root = new BorderPane();
		root.setCenter(createLogInScene());
		root.setBackground(new Background(new BackgroundFill(Color.WHITE, null, null)));

		Scene scene = new Scene(root, 400, 300);
		stage.setScene(scene);
		stage.centerOnScreen(); // Center the stage on the screen
		stage.show();
	}

	public GridPane createLogInScene() {
		// Create the login form
		GridPane loginForm = new GridPane();
		loginForm.setAlignment(Pos.CENTER);
		loginForm.setHgap(10);
		loginForm.setVgap(10);
		loginForm.setPadding(new Insets(25, 25, 25, 25));
		loginForm.setBackground(new Background(new BackgroundFill(Color.WHITE, null, null)));

		// Add the login elements
		Text loginTitle = new Text("Login");
		loginTitle.setFont(Font.font("Tahoma", FontWeight.NORMAL, 20));
		loginTitle.setFill(Color.PURPLE);
		loginForm.add(loginTitle, 0, 0, 2, 1);

		Label emailLabel = new Label("Email:");
		emailLabel.setTextFill(Color.BLACK);
		loginForm.add(emailLabel, 0, 1);

		TextField emailField = new TextField();
		loginForm.add(emailField, 1, 1);

		Label passwordLabel = new Label("Password:");
		passwordLabel.setTextFill(Color.BLACK);
		loginForm.add(passwordLabel, 0, 2);

		PasswordField passwordField = new PasswordField();
		loginForm.add(passwordField, 1, 2);

		Button loginButton = new Button("Login");
		loginButton.setStyle("-fx-background-color: purple; -fx-text-fill: white;");
		loginForm.add(loginButton, 1, 3);

		Hyperlink signUpLink = new Hyperlink("Sign Up");
		signUpLink.setTextFill(Color.BLACK);
		loginForm.add(signUpLink, 1, 4);

		Hyperlink forgotPasswordLink = new Hyperlink("Forgot Password?");
		forgotPasswordLink.setTextFill(Color.BLACK);
		loginForm.add(forgotPasswordLink, 1, 5);

		// Create the login scene
		loginForm.setOnKeyPressed(e -> {
			if (e.getCode().toString().equals("ENTER")) {
				authenticateUser(emailField.getText(), passwordField.getText());
			}
		});
		Scene loginScene = new Scene(loginForm, 400, 300);
		loginButton.setOnMouseClicked(e -> {
		//	System.out.println("Login -->" + emailField.getText().trim() + passwordField.getText().trim());
			authenticateUser(emailField.getText().trim(), passwordField.getText().trim());

			Constants.userID = UserDbHelper.validateUser(emailField.getText().trim(), passwordField.getText().trim());
			if (Constants.userID != -1) {
				MainApp m = new MainApp();
				m.start(stage);
			} else {
				showAlert(AlertType.ERROR, "Invalid Email or password!");
			}

		});

		// Set the forgot password link action
		forgotPasswordLink.setOnAction(e -> {
			// forgot password
			showAlert(AlertType.INFORMATION, "Forgot Password Please contact support to reset your password.");
		});

		// Set the sign up button action
		signUpLink.setOnAction(e -> {
			SignUp signUp = new SignUp();
			signUp.start(stage);

		});

		return loginForm;
	}

	private void showAlert(AlertType type, String message) {
		Alert alert = new Alert(type);
		alert.setTitle("Error");
		alert.setHeaderText(null);
		alert.setContentText(message);
		alert.showAndWait();
	}

	private boolean authenticateUser(String email, String password) {
		// Validate email and password fields
		boolean authenticate = true;
		if (email.isEmpty()) {
			showAlert(AlertType.ERROR, "Email is required.");
			authenticate = false;
		} else if (password.isEmpty()) {
			showAlert(AlertType.ERROR, "Password is required.");
			authenticate = false;
		}

		return authenticate;
	}

}
