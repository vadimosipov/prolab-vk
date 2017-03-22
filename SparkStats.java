package vk;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple3;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class SparkStats {

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("vk_statistics").setMaster("local[4]");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            sc.setLogLevel("WARN");
            JavaRDD<String> data = sc.textFile("/home/vadim/contacts_vk_original3.csv");

            JavaRDD<Tuple3<String, Integer, String>> formatted = format(data);

            Map<String, Long> genderStats = estimateSexArrangement(formatted);
            Map<String, Long> ageStats = estimateAgeArrangement(formatted);
            Map<String, Long> interestsStats = estimateInterestArrangement(sc, formatted);

            printAnswer(genderStats, ageStats, interestsStats);
        }
    }

    static JavaRDD<Tuple3<String, Integer, String>> format(JavaRDD<String> data) {
        Pattern pattern = Pattern.compile(",");
         return data.map(line -> {
            String[] splits = pattern.split(line);

            String interests = splits.length == 3 ? splits[2]
                    : splits.length < 3 ? "-1"
                    : Stream.of(Arrays.copyOfRange(splits, 2, splits.length)).reduce("", (a, b) -> a + "," + b);


            try {
                return new Tuple3<>(splits[0], Integer.valueOf(splits[1]), interests);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("line: " + line);
                return new Tuple3<>("1", 2, "2");
            }
        });
    }

    static Map<String, Long> estimateSexArrangement(JavaRDD<Tuple3<String, Integer, String>> splitted) {
        return splitted.keyBy(Tuple3::_1).countByKey();
    }

    static Map<String, Long> estimateAgeArrangement(JavaRDD<Tuple3<String, Integer, String>> splitted) {
        return splitted.map(t -> {
                            int age = t._2();
                            if (age >= 0 && age <= 10) return "A";
                            else if (age >= 11 && age <= 20) return "B";
                            else if (age >= 21 && age <= 30) return "C";
                            else if (age >= 31) return "D";
                            return "E";
                        })
                        .countByValue();
    }

    static Map<String, Long> estimateInterestArrangement(JavaSparkContext sc, JavaRDD<Tuple3<String, Integer, String>> splitted) throws IOException {
        List<String> russianStopWords = Files.readAllLines(Paths.get("stopwords_ru.txt"));
        List<String> englishStopWords = Files.readAllLines(Paths.get("stopwords_en.txt"));
        Broadcast<HashSet<String>> rsw = sc.broadcast(new HashSet<>(russianStopWords));
        Broadcast<HashSet<String>> esw = sc.broadcast(new HashSet<>(englishStopWords));

        return splitted.map(Tuple3::_3)
                        .filter(interest -> !interest.equals("-1"))
                        .map(line -> line.replaceAll("[\\\"\\)\\(+\\.:;0-9]", ","))
                        .flatMap(word -> Arrays.asList(word.split(",")).iterator())
                        .map(String::toLowerCase)
                        .filter(word -> word.matches("[a-zа-я-_ ]+"))
                        .flatMap(word -> Arrays.asList(word.split(" и ")).iterator())
                        .map(String::trim)
                        .filter(word -> !rsw.getValue().contains(word))
                        .filter(word -> !esw.getValue().contains(word))
                        .countByValue();
    }

    private static void printAnswer(Map<String, Long> genderStats, Map<String, Long> ageStats, Map<String, Long> interestsStats) {
        // т.к. в задании указан не JSON формат ответа (top5_interest не соответствует), будем строить вручную
        StringBuilder builder = new StringBuilder(512);

        StringBuilder age = new StringBuilder(128);
        builder.append("{\n")
                .append("    \"gender\": ")
                .append("{ ")
                .append("\"male\": ").append(genderStats.getOrDefault("2", 0L)).append(", ")
                .append("\"female\": ").append(genderStats.getOrDefault("1", 0L)).append(", ")
                .append("\"?\": ").append(genderStats.getOrDefault("0", 0L))
                .append(" },\n")
                .append("    \"age\": ")
                .append("{ ")
                .append("\"<=10\": ").append(ageStats.getOrDefault("A", 0L)).append(", ")
                .append("\"11-20\": ").append(ageStats.getOrDefault("B", 0L)).append(", ")
                .append("\"21-30\": ").append(ageStats.getOrDefault("C", 0L)).append(", ")
                .append("\"?\": ").append(ageStats.getOrDefault("E", 0L)).append(", ")
                .append("\">=31\": ").append(ageStats.getOrDefault("D", 0L))
                .append(" },\n")
                .append("    \"top5_interest\": ")
                .append("{");

        StringJoiner joiner = new StringJoiner(",");
        interestsStats.entrySet().stream().sorted((o1, o2) -> Long.compare(o2.getValue(), o1.getValue()))
                .limit(5)
                .forEach(e -> joiner.add("\"" + e.getKey() + "\""));
        String answer = builder.append(joiner.toString())
                .append("}\n")
                .append("}")
                .toString();
        System.out.println(answer);
    }
}

/*
   отфильтровать те, которых нету. splits.length == 2 или "-1".equals(split[2])
   каждую строку разбить на ",", "."

   заменить "   ")", "(",  + "." ":", ".", ";" на " "
   нижний регист
   match [a-z\\-]
   если есть и, то разбить по   и    или
   далее trim()
   далее flatMap
   далее пропустить через russian, english стоп слова

    минус в том, что пострадают сайты. Не используется стоп слова по другим языкам
    по пробелам не разбиваем, т.к. фразы а по И можно
    не удалял/не разбивал по "-", а вдруг Санкт-Петербург ? хотя из "в последнее время - улыбаться прохожим", возможно, у кого то есть такой интерес как "улыбаться прохожим" :)
    можно было бы применить морфологический анализ
    разбивал по и, т.к. просто вместе, можно разбить
    разбивал стараюсь все таки не по словам, а именно по интересам, т.к. если разбивать по словам, то меняется смысл таких интересов как "digital october", "digital ocean"
    прочие интересы, от разбиение от которыъ менялся бы смысл
     стараться НЕ заморачиваться
      делать себя лучше
       наступать на грабли
        тупо мечтать
         тонуть в мыслях
         тырить интересы
 */


/*
  "age": { "<=10": 122, "11-20": 123, "21-30": 456, "?": 789, ">=31": 0 },

    val data = sc.textFile("/home/vadim/test_for_vk_spark.txt")
    val splitted = data.map(line => {
      val splits = line.split(",")
      if (splits.length == 3) (splits(0), splits(1), splits(2)) else (splits(0), splits(1), "-1")
    })

//    splitted.collect().foreach(println)


    val filtered = splitted.filter(t => !t._3.equals("-1"))

    println(estimateSex(splitted))


    filtered.collect().foreach(println)
  }

  def estimateSex(splitted: RDD[(String, String, String)]) = {
    splitted.keyBy(t => t._1).countByKey()
  }


/*

    1 — женский;
    2 — мужской;
    0 — пол не указан.

 */

