package TaskManagerProject;

public class PreRequisite {
    private long preTaskId;
    private String taskName;

    public PreRequisite(long preTaskId, String taskName) {
        this.preTaskId = preTaskId;
        this.taskName = taskName;
    }

    public long getPreTaskId() {
        return preTaskId;
    }

    public void setPreTaskId(long preTaskId) {
        this.preTaskId = preTaskId;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    @Override
    public String toString() {
        return "PreRequisite{" +
                "preTaskId=" + preTaskId +
                ", taskName='" + taskName + '\'' +
                '}';
    }
}
