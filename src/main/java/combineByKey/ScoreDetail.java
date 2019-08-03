package combineByKey;

import java.io.Serializable;

public class ScoreDetail implements Serializable {
    private String studentName, subject;
    private int score;

    public ScoreDetail(String studentName, String subject, int score) {
        this.studentName = studentName;
        this.subject = subject;
        this.score = score;
    }

    public ScoreDetail(String studentName, int score) {
        this.studentName = studentName;
        this.score = score;
    }

    public ScoreDetail() {
    }

    public String getStudentName() {
        return studentName;
    }

    public void setStudentName(String studentName) {
        this.studentName = studentName;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }
}