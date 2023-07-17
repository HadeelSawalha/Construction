package TaskManagerProject;

import javafx.application.Application;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.VBox;
import javafx.scene.text.Font;
import javafx.stage.Stage;

public class Welcome extends Application {

	private Stage primaryStage;

	public static void main(String[] args) {
		launch(args);
	}

	@Override
	public void start(Stage primaryStage) {
		this.primaryStage = primaryStage;

		initializeUI();

		primaryStage.setTitle("Welcome");
		primaryStage.show();
	}

	private void initializeUI() {
		BorderPane root = new BorderPane();
		Scene scene = new Scene(root, 1100, 600);

		// Create a VBox to hold the welcome message, image, and start button
		VBox vbox = new VBox(20);
		vbox.setAlignment(Pos.CENTER);

		// Create a label for the welcome message
		Label welcomeLabel = new Label("Welcome to our Task Manager!");
		welcomeLabel.setFont(Font.font("Tahoma", 24));

		// Create an ImageView to display the image
		Image image = new Image("C:\\Users\\HP\\eclipse-workspace\\testFX\\src\\TaskManagerProject\\taskmanager.png");
		ImageView imageView = new ImageView(image);

		// Create a start button
		Button startButton = new Button("Start");
		startButton.setStyle("-fx-font-size: 16px; -fx-padding: 10px 20px;");

		startButton.setOnAction(e -> {
			// Call the login functionality
			Login login = new Login();
			login.start(primaryStage);
		});

		// Add the label, image, and start button to the VBox
		vbox.getChildren().addAll(welcomeLabel, startButton, imageView);

		// Set the VBox as the center of the root pane
		root.setCenter(vbox);

		primaryStage.setScene(scene);
	}

}
