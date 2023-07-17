package TaskManagerProject;

import javafx.application.Application;
import javafx.geometry.*;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.Background;
import javafx.scene.layout.BackgroundFill;
import javafx.scene.layout.GridPane;
import javafx.scene.paint.Color;
import javafx.scene.text.*;
import javafx.stage.Stage;
import javafx.scene.control.Alert.AlertType;

public class SignUp extends Application {
	private Stage stage;

	@Override
	public void start(Stage stage) {
		stage.setTitle("SignUp");
		this.stage = stage;
		// Show the sign-up scene
		stage.setScene(createSignUpScene());
		stage.show();
	}

	public Scene createSignUpScene() {
		// Create sign-up form
		GridPane signUpForm = new GridPane();
		signUpForm.setAlignment(Pos.CENTER);
		signUpForm.setHgap(10);
		signUpForm.setVgap(10);
		signUpForm.setPadding(new Insets(30, 30, 30, 30));
		signUpForm.setBackground(new Background(new BackgroundFill(Color.WHITE, null, null)));

		// Add the sign up elements
		Text signUpTitle = new Text("SignUp");
		signUpTitle.setFont(Font.font("Tahoma", FontWeight.NORMAL, 20));
		signUpTitle.setFill(Color.PURPLE);
		signUpForm.add(signUpTitle, 0, 0, 2, 1);

		Label firstNameLabel = new Label("First Name:");
		firstNameLabel.setTextFill(Color.BLACK);
		signUpForm.add(firstNameLabel, 0, 1);

		TextField firstNameField = new TextField();
		signUpForm.add(firstNameField, 1, 1);

		Label lastNameLabel = new Label("Last Name:");
		lastNameLabel.setTextFill(Color.BLACK);
		signUpForm.add(lastNameLabel, 0, 2);

		TextField lastNameField = new TextField();
		signUpForm.add(lastNameField, 1, 2);

		Label emailLabel = new Label("Email:");
		emailLabel.setTextFill(Color.BLACK);
		signUpForm.add(emailLabel, 0, 3);

		TextField emailField = new TextField();
		signUpForm.add(emailField, 1, 3);

		Label passwordLabel = new Label("Password:");
		passwordLabel.setTextFill(Color.BLACK);
		signUpForm.add(passwordLabel, 0, 4);

		PasswordField passwordField = new PasswordField();
		signUpForm.add(passwordField, 1, 4);

		Label confirmPasswordLabel = new Label("Confirm Password:");
		confirmPasswordLabel.setTextFill(Color.BLACK);
		signUpForm.add(confirmPasswordLabel, 0, 5);

		PasswordField confirmPasswordField = new PasswordField();
		signUpForm.add(confirmPasswordField, 1, 5);

		Button signUpButton = new Button("Sign Up");
		signUpButton.setStyle("-fx-background-color: purple; -fx-text-fill: white;");
		signUpForm.add(signUpButton, 1, 6);

		// Create signUp scene
		Scene signUpScene = new Scene(signUpForm, 400, 300);

		// signUp button action
		signUpButton.setOnAction(e -> {
			// Create user account
			String firstName = firstNameField.getText();
			String lastName = lastNameField.getText();
			String email = emailField.getText();
			String password = passwordField.getText();
			String confirmPassword = confirmPasswordField.getText();

			if (firstName.isEmpty() || lastName.isEmpty() || email.isEmpty() || password.isEmpty()
					|| confirmPassword.isEmpty()) {
				showAlert(AlertType.ERROR, "Please fill out all fields!");
			} else if (!password.equals(confirmPassword)) {
				showAlert(AlertType.ERROR, "Passwords do not match!");
			} else if (!email.matches("[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}")) {
				showAlert(AlertType.ERROR, "Email should match the email form.");
			} else if (password.length() <= 8) {
				showAlert(AlertType.ERROR, "Password should contain at least 8 characters.");
			} else {
				//System.out.println("SignUp-->" + firstName + " " + lastName + " " + email + " " + password + " User");
				User user = new User(firstName, lastName, email, password, "User");
				UserDbHelper.createUser(user);
				Login login = new Login();
				login.start(stage);
			}

		});

		return signUpScene;
	}

	private void showAlert(AlertType type, String message) {
		Alert alert = new Alert(type);
		alert.setTitle("Error");
		alert.setHeaderText(null);
		alert.setContentText(message);
		alert.showAndWait();
	}
	public static void main(String[] args) {
		launch(args);
	}

}
