package hadoop;

import domain.AvgScore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Hadoop {
    static Long start;
    private static final Map<String, Double> ratingsGlobal = new HashMap<>();
    private static final Map<String, AvgScore> directorsAndFilmsGlobal = new HashMap<>();

    public static class CrewMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void setup(Context context) throws IOException {
            start = System.currentTimeMillis();
            try (BufferedReader br = new BufferedReader(new FileReader(context.getConfiguration().get("ratingsFileName")))) {
                String line;
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
                    ratingsGlobal.put(tconst, averageRating);
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) {
            try {
                String[] parts = value.toString().split("\t");
                String tconst = parts[0];
                if (tconst.equals("tconst"))
                    return;
                String[] directors = parts[1].split(",");
                for (String director : directors) {
                    if (director.equals("\\N")) break;
                    context.write(new Text(tconst), new Text(director + "," + tconst));
                }
            } catch (Exception e) {
                System.out.println(e);
            }
        }
    }

    public static class AverageCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            performReduce(values);
        }
    }


    public static class AverageReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) {
            performReduce(values);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String s : directorsAndFilmsGlobal.keySet()) {
                Double avgForDirector = directorsAndFilmsGlobal.get(s).getAvgForDirector();
                System.out.println(s + ", " + avgForDirector);
            }
            Long end = System.currentTimeMillis();
            System.out.println("Time:" + (end - start)+ " ms");
        }
    }

    private static void performReduce(Iterable<Text> values) {
        for (Text value : values) {
            String tconst = value.toString().split(",")[1];
            String director = value.toString().split(",")[0];
            Double rating;
            if ((rating = ratingsGlobal.get(tconst)) != null) {
                AvgScore item = directorsAndFilmsGlobal.get(director);
                if (item == null) {
                    directorsAndFilmsGlobal.put(director, new AvgScore(rating, 1));
                } else {
                    directorsAndFilmsGlobal.put(director, new AvgScore(item.getAvgSum() + rating, item.getCounter() + 1));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("ratingsFileName", "title.ratings.tsv");
        Job job = Job.getInstance(conf, "average_rating_by_director");
        job.setJarByClass(Hadoop.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(CrewMapper.class);
        job.setCombinerClass(AverageCombiner.class);
        job.setReducerClass(AverageReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}

