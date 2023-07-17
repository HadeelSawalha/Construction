package TaskManagerProject;

import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.GridPane;
import javafx.scene.paint.Color;
import javafx.stage.Modality;
import javafx.stage.Stage;

import java.time.LocalDate;

public class EditTaskDialog extends Dialog<Task> {

    private Task task;
    private TextField nameField;
    private TextField tagField;
    private TextField descArea;
    private DatePicker datePicker;
    private ComboBox<Integer> priorityComboBox;
    private ComboBox<Status> statusComboBox;
    private ComboBox<Category> categoryComboBox;
    private ButtonType saveButton;
    private ButtonType cancelButton;

    public EditTaskDialog(Task task) {
        this.task = task;
        setTitle("Edit Task");
        setHeaderText("Edit the task details");

        // Set the task details as the default values in the dialog
        nameField = new TextField(task.getTaskName());
        descArea = new TextField(task.getTaskDescription());
        datePicker = new DatePicker(task.getDueDate());
        priorityComboBox = new ComboBox<>();
        priorityComboBox.getItems().addAll(1, 2, 3, 4, 5);
        priorityComboBox.setValue(task.getPriorityLevel());
        statusComboBox = new ComboBox<>();
        statusComboBox.getItems().addAll(Status.values());
        statusComboBox.setValue(Status.valueOf(task.getStatus() + ""));
        categoryComboBox = new ComboBox<>();
        categoryComboBox.getItems().addAll(Category.values());
        categoryComboBox.setValue(Category.valueOf(task.getCategory() + ""));

        // Create the save button
        saveButton = new ButtonType("Save", ButtonBar.ButtonData.OK_DONE);
        cancelButton = new ButtonType("Cancel", ButtonBar.ButtonData.CANCEL_CLOSE);

        getDialogPane().getButtonTypes().addAll(saveButton, cancelButton);

        // Create the layout
        GridPane grid = new GridPane();
        grid.setAlignment(Pos.CENTER);
        grid.setHgap(10);
        grid.setVgap(10);
        grid.setPadding(new Insets(40));

        Label nameLabel = new Label("Name:");
        nameLabel.setTextFill(Color.BLACK);
        nameLabel.setStyle(Constants.labelStyle);
        grid.add(nameLabel, 0, 0);
        grid.add(nameField, 1, 0);

        Label descLabel = new Label("Description:");
        descLabel.setTextFill(Color.BLACK);
        descLabel.setStyle(Constants.labelStyle);
        grid.add(descLabel, 0, 1);
        grid.add(descArea, 1, 1);

        Label dueDateLabel = new Label("Due Date:");
        dueDateLabel.setTextFill(Color.BLACK);
        dueDateLabel.setStyle(Constants.labelStyle);
        grid.add(dueDateLabel, 0, 2);
        grid.add(datePicker, 1, 2);

        Label priorityLabel = new Label("Priority:");
        priorityLabel.setTextFill(Color.BLACK);
        priorityLabel.setStyle(Constants.labelStyle);
        grid.add(priorityLabel, 0, 3);
        grid.add(priorityComboBox, 1, 3);

        Label statusLabel = new Label("Status:");
        statusLabel.setTextFill(Color.BLACK);
        statusLabel.setStyle(Constants.labelStyle);
        grid.add(statusLabel, 0, 4);
        grid.add(statusComboBox, 1, 4);

        Label categoryLabel = new Label("Category:");
        categoryLabel.setTextFill(Color.BLACK);
        categoryLabel.setStyle(Constants.labelStyle);
        grid.add(categoryLabel, 0, 5);
        grid.add(categoryComboBox, 1, 5);

        getDialogPane().setContent(grid);

        Button saveButtonControl = (Button) getDialogPane().lookupButton(saveButton);
        saveButtonControl.setStyle(Constants.buttonStyle);

        Button cancleButtonControl = (Button) getDialogPane().lookupButton(cancelButton);
        cancleButtonControl.setStyle(Constants.buttonStyle);
        
        // Convert the result to a Task object when the save button is clicked
        setResultConverter(buttonType -> {
            if (buttonType == saveButton) {
                updateTask();
                return task;
            }
            return null;
        });
    }

    private void updateTask() {
        task.setTaskName(nameField.getText());
        task.setTaskDescription(descArea.getText());
        task.setDueDate(datePicker.getValue());
        task.setPriorityLevel(priorityComboBox.getValue());
        task.setStatus(statusComboBox.getValue());
        task.setCategory(categoryComboBox.getValue());
       // for Test : System.out.println("Edit --->"+task.toString());
        TaskDBHelper.updateTaskName(task.getTaskId(), task.getTaskName());
        TaskDBHelper.updateTaskDescription(task.getTaskId(), task.getTaskDescription());
        TaskDBHelper.updateTaskDeadline(task.getTaskId(), task.getDueDate());
        TaskDBHelper.updateTaskPriorityLevel(task.getTaskId(), task.getPriorityLevel());
        TaskDBHelper.updateTaskStatus(task.getTaskId(), task.getStatus().toString());
        TaskDBHelper.updateTaskCategory(task.getTaskId(), task.getCategory().toString());
    }
}
