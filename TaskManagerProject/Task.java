 package TaskManagerProject;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class Task {

	private long taskId;
	private String taskName;
	private String taskDescription;
	private String tag;
	private LocalDate dueDate;
	private int priorityLevel;
	private Status status;
	private ArrayList<PreRequisite> preRequisites;
	private long assignedUserId;
	private long createdByUserId;
	private Category category;

	public Task(long taskId, String taskName, String taskDescription, String tag, LocalDate dueDate, int priorityLevel,
			ArrayList<PreRequisite> preRequisites, long assignedUserId, long createdByUserId, String category,
			String status) {
		this.taskId = taskId;
		this.taskName = taskName;
		this.taskDescription = taskDescription;
		this.tag = tag;
		this.dueDate = dueDate;
		this.priorityLevel = priorityLevel;
		this.preRequisites = preRequisites;
		this.assignedUserId = assignedUserId;
		this.createdByUserId = createdByUserId;
		this.category = Category.valueOf(category);
		this.status = Status.valueOf(status);
	}

	public Task(long taskId, String taskName, LocalDate dueDate, Status status) {
		this.taskId = taskId;
		this.taskName = taskName;
		this.dueDate = dueDate;
		this.status = status;
	}

	public long getTaskId() {
		return taskId;
	}

	public void setTaskId(long taskId) {
		this.taskId = taskId;
	}

	public String getTaskName() {
		return taskName;
	}

	public void setTaskName(String taskName) {
		this.taskName = taskName;
	}

	public String getTaskDescription() {
		return taskDescription;
	}

	public void setTaskDescription(String taskDescription) {
		this.taskDescription = taskDescription;
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public LocalDate getDueDate() {
		return dueDate;
	}

	public void setDueDate(LocalDate dueDate) {
		this.dueDate = dueDate;
	}

	public int getPriorityLevel() {
		return priorityLevel;
	}

	public void setPriorityLevel(int priorityLevel) {
		this.priorityLevel = priorityLevel;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public ArrayList<PreRequisite> getPreRequisites() {
		return preRequisites;
	}

	public void setPreRequisites(ArrayList<PreRequisite> preRequisites) {
		this.preRequisites = preRequisites;
	}

	public long getAssignedUserId() {
		return assignedUserId;
	}

	public void setAssignedUserId(long assignedUserId) {
		this.assignedUserId = assignedUserId;
	}

	public long getCreatedByUserId() {
		return createdByUserId;
	}

	public void setCreatedByUserId(long createdByUserId) {
		this.createdByUserId = createdByUserId;
	}

	public Category getCategory() {
		return category;
	}

	public void setCategory(Category category) {
		this.category = category;
	}

	@Override
	public String toString() {
		return "Task{" + "taskId=" + taskId + ", taskName='" + taskName + '\'' + ", taskDescription='" + taskDescription
				+ '\'' + ", tag='" + tag + '\'' + ", dueDate=" + dueDate + ", priorityLevel=" + priorityLevel
				+ ", status=" + status + ", preRequisites=" + preRequisites + ", assignedUserId=" + assignedUserId
				+ ", createdByUserId=" + createdByUserId + ", category=" + category + '}';
	}
}
