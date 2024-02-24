import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;


public class IdGenerator {
    private static final String FILE_NAME = "incrementingId.txt";
    private static int id;

    static {
        loadLastId();
    }

    public static String getId() {
        String idAsString = Integer.toString(id);
        id = id + 1;
        saveLastId();
        return idAsString;
    }

   private static void loadLastId() {
    try {
        File file = new File(FILE_NAME);

        if (!file.exists()) {
            file.createNewFile();
            id = 1;
            saveLastId();
        } else {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line = reader.readLine();
                if (line != null && !line.isEmpty()) {
                    id = Integer.parseInt(line);
                }
            }
        }
    } catch (IOException | NumberFormatException e) {
        e.printStackTrace();
    }
}


    private static void saveLastId() {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(FILE_NAME))) {
            writer.write(Integer.toString(id));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
