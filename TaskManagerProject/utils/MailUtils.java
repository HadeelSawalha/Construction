package utils;

import TaskManagerProject.*;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.ArrayList;
import java.util.Properties;

public class MailUtils {

    private static final String username = "notification.tms.app@gmail.com";
    private static final String password = "tnodpbsgtiyhtdim";

    private static final Properties prop = new Properties() {
        {
            put("mail.smtp.host", "smtp.gmail.com");
            put("mail.smtp.port", "465");
            put("mail.smtp.auth", "true");
            put("mail.smtp.socketFactory.port", "465");
            put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
        }
    };
    public static void sendEmail(String recipient, String subject, String body) {
        Session session = Session.getInstance(prop,
                new javax.mail.Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(username, password);
                    }
                });

        try {

            Message message = new MimeMessage(session);
            message.setContent(body, "text/html; charset=utf-8");
            message.setFrom(new InternetAddress(username));
            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(recipient));
            message.setSubject(subject);

            Transport.send(message);

            System.out.println("Done");
        } catch (MessagingException e) {
            e.printStackTrace();
        }
    }

    public static String buildTaskAssignedBody(String username, ArrayList<Task> tasks) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("<p>Hi %s,<br /><br />You have been assigned following task:</p>", username));
                for (Task task:tasks) {
                    sb.append(String.format("<p><strong>%s: %s</strong></p>", task.getTaskId(), task.getTaskName()));
                }
        sb.append("<p>Best Regards,<br />TMS Team</p>");

        return sb.toString();
    }
}
