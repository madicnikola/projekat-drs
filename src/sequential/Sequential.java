package sequential;

import domain.AvgScore;

import java.io.*;
import java.util.*;

public class Sequential {

    private static final Map<String, Double> ratingsMap = new HashMap<>();
    private static final Map<String, AvgScore> directorsFilmsMap = new HashMap<>();

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.out.println("Java director movies average rating title.ratings.tsv title.crew.tsv");
            return;
        }
        // Read the title.ratings.tsv file
        try (BufferedReader br = new BufferedReader(new FileReader(args[0]))) {
            String line;
            // skip header
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\t");
                String tconst = parts[0];
                double averageRating = 0.0;
                if (!parts[1].equals("\\N")) {
                    try {
                        averageRating = Double.parseDouble(parts[1]);
                    } catch (Exception e) {
                        System.out.println(e);
                    }
                }
                ratingsMap.put(tconst, averageRating);
            }
        }

        // Read the title.crew.tsv file
        try (BufferedReader br = new BufferedReader(new FileReader(args[1]))) {
            String line;
            // skip header
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\t");
                String tconst = parts[0];
                String[] directors = parts[1].split(",");
                for (String director : directors) {
                    if (director.equals("\\N")) break;
                    Double rating;
                    if ((rating = ratingsMap.get(tconst)) != null) {
                        AvgScore item = directorsFilmsMap.get(director);
                        if (item == null) {
                            directorsFilmsMap.put(director, new AvgScore(rating, 1));
                        } else {
                            directorsFilmsMap.put(director, new AvgScore(item.getAvgSum() + rating, item.getCounter() + 1));
                        }
                    }
                }
            }

            for (String s : directorsFilmsMap.keySet()) {
                Double avgForDirector = directorsFilmsMap.get(s).getAvgForDirector();
                System.out.println(s + ", " + avgForDirector);
            }
        }
    }
}

