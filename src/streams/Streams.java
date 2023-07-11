package streams;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;


import domain.AvgScore;
import domain.Crew;
import domain.Rating;

class Streams {

    private static Map<String, Double> ratingsMap = new HashMap<>();

    public static void main(String[] args) {
        loadRatings();
        try (Stream<String> stream = Files.lines(Paths.get("title.crew.tsv"))) {
            Map<String, AvgScore> directorsMoviesAvg = stream
                    .skip(1)
                    .flatMap(crew -> {
                        String[] fields = crew.split("\t");
                        List<Crew> list = new ArrayList<>();
                        if (fields[1].equals("\\N"))
                            return list.stream();
                        for (int i = 0; i < fields[1].split(",").length; i++) {
                            list.add(new Crew(fields[0], fields[1].split(",")[i]));
                        }
                        return list.stream();
                    })
                    .filter(crew -> (!crew.director.equals("\\N")) && (ratingsMap.get(crew.tconst) != null))
                    .collect(Collectors.toMap
                            (c -> c.director,
                                    c -> new AvgScore(ratingsMap.get(c.tconst), 1),
                                    (a, b) -> new AvgScore(a.getAvgSum() + b.getAvgSum(), a.getCounter() + b.getCounter())));

            // Print
            for (String s : directorsMoviesAvg.keySet()) {
                Double avgForDirector = directorsMoviesAvg.get(s).getAvgForDirector();
                System.out.println(s + ", " + avgForDirector);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void loadRatings() {
        try (Stream<String> ratingsStream = Files.lines(Paths.get("title.ratings.tsv"))) {
            ratingsMap = ratingsStream
                    .skip(1)
                    .map(Rating::new)
                    .collect(Collectors.toMap(r -> r.tconst, r -> r.averageRating));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

