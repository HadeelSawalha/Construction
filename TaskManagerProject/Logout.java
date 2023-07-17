package TaskManagerProject;

import java.util.Optional;

import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.GridPane;
import javafx.stage.Screen;
import javafx.stage.Stage;

public class Logout extends BorderPane {
	GridPane logout = new GridPane();
	Button logoutButton = new Button("Logout");
	Label logoutLabel = new Label("Bye Bye!!");

	public Logout(Stage primaryStage, Scene scene) {
		logoutButton.setStyle(Constants.buttonStyle);
		logoutLabel.setStyle(Constants.labelStyle);
		logoutButton.setOnAction(event -> primaryStage.close());
		
		logout.add(logoutLabel, 0, 1);
		logout.add(logoutButton, 1, 1);

		setCenter(logout);
		logout.setAlignment(Pos.CENTER);
		logout.setVgap(15);
		logout.setHgap(15);

	}
}
