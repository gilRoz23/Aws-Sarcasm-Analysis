import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentAnnotatedTree;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

public class StanfordActivator {

    final Properties sentimentProps;
    final StanfordCoreNLP sentimentPipeline;
    final Properties entityProps;
    final StanfordCoreNLP NERPipeline;



    public StanfordActivator() {

        sentimentProps = new Properties();
        sentimentProps.put("annotators", "tokenize, ssplit, parse, sentiment");
        sentimentPipeline = new StanfordCoreNLP(sentimentProps);
        entityProps = new Properties();
        entityProps.put("annotators", "tokenize , ssplit, pos, lemma, ner");
        NERPipeline = new StanfordCoreNLP(entityProps);
    }

    public ProcessedReview process(Review review) {
        List<String> entities = getEntities(review.getText());
        int sentiment = findSentiment(review.getText());
        return new ProcessedReview(review.getLink(), convertSentimentRatingToColor(sentiment), entities, isSarcasm(review.getRating(), sentiment));
    }

    private Color convertSentimentRatingToColor(int sentiment) {
        try {
            switch (sentiment) {
                case 0:
                    return Color.DARKRED;
                case 1:
                    return Color.RED;
                case 2:
                    return Color.BLACK;
                case 3:
                    return Color.LIGHTGREEN;
                case 4:
                    return Color.DARKGREEN;
                default:
                    throw new Exception("impossible sentiment rating: " + sentiment);

            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return null;
        }

    }

    private boolean isSarcasm(long rating, int sentiment) {
        if (sentiment >= 3 && rating <= 2)
            return true;
        else return sentiment <= 1 && rating >= 4;
    }

    private List<String> getEntities(String review) {
        List<String> entities = new LinkedList<>();
        Annotation document = new Annotation(review);
        NERPipeline.annotate(document);
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);
        for (CoreMap sentence : sentences) {
            for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
                String word = token.get(TextAnnotation.class);
                String ne = token.get(NamedEntityTagAnnotation.class);
                if (ne.equals("LOCATION") || ne.equals("ORGANIZATION") || ne.equals("PERSON"))
                    entities.add(word);
            }
        }
        return entities;
    }

    private int findSentiment(String review) {
        int mainSentiment = 0;
        if (review != null && !review.isEmpty()) {
            int longest = 0;
            Annotation annotation = sentimentPipeline.process(review);
            for (CoreMap sentence : annotation
                    .get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = sentence.get(
                        SentimentAnnotatedTree.class); 
                int sentiment = edu.stanford.nlp.neural.rnn.RNNCoreAnnotations.getPredictedClass(tree); 
                String partText = sentence.toString();
                if (partText.length() > longest) {
                    mainSentiment = sentiment;
                    longest = partText.length();
                }
            }
        }
        return mainSentiment;
    }



}
