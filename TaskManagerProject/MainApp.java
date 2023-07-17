package TaskManagerProject;

import java.util.List;

import atlantafx.base.theme.PrimerLight;
import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.control.Label;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.text.Font;
import javafx.stage.Stage;

public class MainApp extends Application {

	public static void main(String[] args) {
		launch(args);
	}

	@Override
	public void start(Stage primaryStage) {
		// Initialize tabPane
		TabPane tabPane = new TabPane();
		Scene scene = new Scene(tabPane, 1100, 700);

		Tab ViewTap = new Tab("Task View");
		Tab CreateTap = new Tab("Create Task");
		Tab SearchTap = new Tab("Search");
		Tab FiltersTap = new Tab("Filters");
		Tab NotificationTap = new Tab("Notification");
		Tab CalendarTap = new Tab("Calendar");
		Tab userViewTap = new Tab("User List");
		// Tab ReportTap = new Tab("Report");
		Tab LogoutTap = new Tab("Logout");

		// tabs style
		ViewTap.setStyle(Constants.tabStyle);
		CreateTap.setStyle(Constants.tabStyle);
		SearchTap.setStyle(Constants.tabStyle);
		FiltersTap.setStyle(Constants.tabStyle);
		NotificationTap.setStyle(Constants.tabStyle);
		CalendarTap.setStyle(Constants.tabStyle);
		// ReportTap.setStyle(Constants.tabStyle);
		LogoutTap.setStyle(Constants.tabStyle);
		userViewTap.setStyle(Constants.tabStyle);

		TaskView view = new TaskView(primaryStage, scene);
		CreateTask create = new CreateTask(primaryStage, scene);
		SearchTask searchTask = new SearchTask(primaryStage, scene);
		FilterTask filter = new FilterTask(primaryStage, scene);
		Notification notifi = new Notification(primaryStage, scene);
		TaskCalendar calendar = new TaskCalendar(primaryStage, scene);
		UserView userview = new UserView(primaryStage, scene);
		Logout logout = new Logout(primaryStage, scene);

		
		ViewTap.setContent(view);
		CreateTap.setContent(create);
		SearchTap.setContent(searchTask);
		FiltersTap.setContent(filter);
		NotificationTap.setContent(notifi);
		CalendarTap.setContent(calendar);
		userViewTap.setContent(userview);
		LogoutTap.setContent(logout);

		ViewTap.setClosable(false);
		CreateTap.setClosable(false);
		SearchTap.setClosable(false);
		FiltersTap.setClosable(false);
		NotificationTap.setClosable(false);
		CalendarTap.setClosable(false);
		// ReportTap.setClosable(false);
		userViewTap.setClosable(false);
		LogoutTap.setClosable(false);

		tabPane.getTabs().addAll(ViewTap, CreateTap, SearchTap, FiltersTap, userViewTap, NotificationTap, CalendarTap,
				/* ReportTap, */ LogoutTap);
		// external jar to css for design
		Application.setUserAgentStylesheet(new PrimerLight().getUserAgentStylesheet());

		primaryStage.setTitle("Task Manager");
		primaryStage.setScene(scene);
		primaryStage.show();

	}

}
