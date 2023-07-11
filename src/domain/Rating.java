package domain;

public class Rating {
    public String tconst;
    public Double averageRating;


    public Rating(String line) {
        String[] columns = line.split("\t");
        this.tconst = columns[0];
        if (!"\\N".equals(columns[1])) {
            this.averageRating = Double.valueOf(columns[1]);
        }
    }

    public Rating(String tconst, Double averageRating) {
        this.tconst = tconst;
        this.averageRating = averageRating;
    }
}
