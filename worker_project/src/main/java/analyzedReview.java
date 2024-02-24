
import java.util.List;

public class analyzedReview {


    private String link;
    private Color color;
    private List<String> namedEntities;
    private boolean isSarcastic;

    public analyzedReview(String link, Color color, List<String> ent, boolean sarcasm) {
        this.link = link;
        this.color = color;
        this.namedEntities = ent;
        this.isSarcastic = sarcasm;
    }

    public String toString() {
        return "ProcessedReview{" +
                "link='" + link + '\'' +
                ", color=" + color +
                ", namedEntities=" + namedEntities +
                ", isSarcastic=" + isSarcastic +
                '}';
    }

    public String getLink() {
        return link;
    }

    // Getter for color
    public Color getColor() {
        return color;
    }

    // Getter for namedEntities
    public List<String> getNamedEntities() {
        return namedEntities;
    }

    // Getter for isSarcastic
    public boolean isSarcastic() {
        return isSarcastic;
    }
}