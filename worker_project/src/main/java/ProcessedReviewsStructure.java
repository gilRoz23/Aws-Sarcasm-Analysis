import java.io.IOException;
import java.util.*;

import java.io.File;
import java.io.FileWriter;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class ProcessedReviewsStructure {

    private List<ProcessedReview> analyzedReviews;

    public ProcessedReviewsStructure() {
        analyzedReviews = new ArrayList();
    }

    public void OutputToJSONFile(String path) {
        JSONObject obj = new JSONObject();
        JSONArray procReviews = new JSONArray();
        Iterator<ProcessedReview> it = analyzedReviews.iterator();
        for (int i = 0; i < analyzedReviews.size(); i++) {
            JSONObject entry = new JSONObject();
            ProcessedReview pr = it.next();
            entry.put("link", pr.getLink());
            entry.put("color", pr.getColor().name());
            JSONArray allEntities = new JSONArray();
            allEntities.addAll(pr.getNamedEntities());
            entry.put("namedEntities", allEntities);
            entry.put("isSarcastic", pr.isSarcastic());
            procReviews.add(entry);
        }
        obj.put("processedEntries", procReviews);

        try {
            FileWriter file = new FileWriter(path);
            file.write(obj.toJSONString());
            file.flush();
            file.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void OutputToHTMLFile(String fileName) {
        try {
            FileWriter file = new FileWriter(fileName);
            file.write("<html>\n<head>\n</head>\n<body>\n");
            for (ProcessedReview pr : analyzedReviews) {
                String color = pr.getColor().name().toLowerCase();
                file.write("<div style=\"color: " + color + ";\">\n");
                file.write("<p>Link: <a href=\"" + pr.getLink() + "\">" + pr.getLink() + "</a></p>\n");
                file.write("<p>Named Entities: [" + String.join(", ", pr.getNamedEntities()) + "]</p>\n");
                file.write("<p>Sarcasm Detection: " + (pr.isSarcastic() ? "Sarcastic" : "Not Sarcastic") + "</p>\n");
                file.write("</div>\n");
            }

            file.write("</body>\n</html>");
            file.flush();
            file.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void appendProcessedReview(ProcessedReview pr) {
        analyzedReviews.add(pr);
    }

    public void appendProcessedReviews(List<ProcessedReview> pr) {
        analyzedReviews.addAll(pr);
    }

    public String processedReviewsToJSONString() {
        JSONObject obj = new JSONObject();
        JSONArray procReviews = new JSONArray();
        Iterator<ProcessedReview> it = analyzedReviews.iterator();

        for (int i = 0; i < analyzedReviews.size(); i++) {
            JSONObject entry = new JSONObject();
            ProcessedReview pr = it.next();
            entry.put("link", pr.getLink());
            entry.put("color", pr.getColor().name());
            JSONArray allEntities = new JSONArray();
            allEntities.addAll(pr.getNamedEntities());
            entry.put("namedEntities", allEntities);
            entry.put("isSarcastic", pr.isSarcastic());
            procReviews.add(entry);
        }
        obj.put("processedEntries", procReviews);

        return obj.toJSONString();
    }

}