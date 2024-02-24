
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.json.simple.*;
import org.json.simple.parser.JSONParser;

public class Input {
    private String title;
    private List<Review> reviews;

    private Input()
    {}

    
    public static Input StringToInput(String reviewString){
        JSONParser parser = new JSONParser();
        Input cur = new Input();
        cur.reviews = new LinkedList<Review>();
        try {
        Object jsonObj = parser.parse(reviewString);
        JSONObject jsonObject = (JSONObject) jsonObj;
        cur.title = (String) jsonObject.get("title");
        Iterator<JSONObject> it = ((JSONArray) jsonObject.get("reviews")).iterator(); // the reason is that hte value of jsonObject.get(reviews)= List(jasonObjects)
        while (it.hasNext()) {
            Review r = new Review(it.next());
            cur.reviews.add(r);
        }
    } catch(Exception e) {
        e.printStackTrace();
    }
        return cur ;
            
    }

    public static List<Input> FileToInput(String path) {
        List<Input> ans = new LinkedList<Input>();

        File inputFile = new File(path);
        try{

            JSONParser parser = new JSONParser();
            Reader reader = new FileReader(inputFile);
            List<String> inputTextLines = Files.readAllLines(Paths.get(inputFile.getAbsolutePath()));
            for(String line: inputTextLines){
                Input cur = new Input();
                cur.reviews = new LinkedList<Review>();
                Object jsonObj = parser.parse(line);
                JSONObject jsonObject = (JSONObject) jsonObj;
                cur.title = (String) jsonObject.get("title");
                Iterator<JSONObject> it = ((JSONArray) jsonObject.get("reviews")).iterator(); 
                while (it.hasNext()) {
                    Review r = new Review(it.next());
                    cur.reviews.add(r);
                }
                ans.add(cur);
            }
            reader.close();
        } catch(Exception e) {
            e.printStackTrace();
        }

        return ans;
    }

    public String getTitle() {
        return title;
    }

    public List<Review> getReviews() {
        return reviews;
    }

    public String toString() {
        return "Input{" +
                "title='" + title + '\'' +
                ", reviews=" + reviews +
                '}';
    }
}
