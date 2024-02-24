
import org.json.simple.JSONObject;

public class Review {
    private String id;
    private String link;
    private String title;
    private String text;
    private Long rating;
    private String author;
    private String date;

    public Review(String id, String link, String title, String text, Long rating, String author, String date) {
        this.id = id;
        this.link = link;
        this.title = title;
        this.text = text;
        this.rating = rating;
        this.author = author;
        this.date = date;
    }

    public Review(JSONObject jo) {
        this.id = (String)jo.get("id");
        this.link = (String) jo.get("link");
        this.title = (String) jo.get("title");
        this.text = (String) jo.get("text");
        this.rating = (Long) jo.get("rating");
        this.author = (String) jo.get("author");
        this.date = (String) jo.get("date");
    }

    public String getId() {
        return id;
    }

    // Getter for link
    public String getLink() {
        return link;
    }

    // Getter for title
    public String getTitle() {
        return title;
    }

    // Getter for text
    public String getText() {
        return text;
    }

    // Getter for rating
    public Long getRating() {
        return rating;
    }

    // Getter for author
    public String getAuthor() {
        return author;
    }

    // Getter for date
    public String getDate() {
        return date;
    }

    public String toString() {
        return "Review{" +
                "id='" + id + '\'' +
                ", link='" + link + '\'' +
                ", title='" + title + '\'' +
                ", text='" + text + '\'' +
                ", rating=" + rating +
                ", author='" + author + '\'' +
                ", date='" + date + '\'' +
                '}';
    }
}