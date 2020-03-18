import java.applet.AppletStub;
import java.io.UnsupportedEncodingException;
import java.lang.invoke.SwitchPoint;
import java.security.SecureRandom;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.*;

public class DataBaseTransformer {
    public static Map<String, Integer> dictionary = new HashMap<String, Integer>();
    public static Map<Integer, String[]> base = new HashMap<Integer, String[]>();

    private static final String CHAR_LOWER = "abcdefghijklmnopqrstuvwxyz";
    private static final String CHAR_UPPER = CHAR_LOWER.toUpperCase();
    private static final String NUMBER = "0123456789";
    private static final String DATA_FOR_RANDOM_STRING = CHAR_LOWER + CHAR_UPPER + NUMBER;
    private static SecureRandom random = new SecureRandom();

    public static void main(String[] args) {
        try {
            Connection connection = DriverManager.getConnection("jdbc:postgresql://database-1.cdm5bcwbgp5i.us-east-1.rds.amazonaws.com:5432/tweet_db", "postgres", "12345123");
            LocalDateTime localDateTime = LocalDateTime.now();
            int day = localDateTime.getDayOfMonth() - 1;
            int month = localDateTime.getMonth().getValue();
            Map<Integer, String> months = new HashMap<>();
            months.put(1, "Jan");
            months.put(2, "Feb");
            months.put(3, "Mar");
            months.put(4, "Apr");
            months.put(5, "May");
            months.put(6, "Jun");
            months.put(7, "Jul");
            months.put(8, "Aug");
            months.put(9, "Sep");
            months.put(10, "Oct");
            months.put(11, "Now");
            months.put(12, "Dec");
            String sql = "SELECT text from tweets_2 where day = ? and month = ? order by tweet_id DESC";

            PreparedStatement statement = connection.prepareStatement(sql);
            statement.setString(1, "" + 15);
            statement.setString(2, months.get(month));
            ResultSet resultSet = statement.executeQuery();
            Integer i = 0;
            List<String> strings;
            while (resultSet.next()) {
                strings = new ArrayList<>();
                String text = resultSet.getString("text");
                String[] words = text.split("\\s*(\\s|,|!|\\.)\\s*");
                StringBuilder sb = new StringBuilder();
                for (int k = 0; k < words.length; k++) {
                    sb.setLength(0);
                    words[k] = words[k].toLowerCase();
                    for (int j = 0; j < words[k].length(); j++) {
                        if (Character.isAlphabetic(words[k].charAt(j))) {
                            sb.append(words[k].charAt(j));
                        }
                    }
                    if (sb.length() != 0) {
                        strings.add(sb.toString());
                    }
                }
                base.put(i, strings.toArray(new String[strings.size()]));
                i++;
            }
            processData();
            String name = "tweets_last_version";
            //language=sql
            String monthDay = localDateTime.getMonth().getValue() + "_" + (15);
            sql = "CREATE TABLE IF NOT EXISTS " + name + " (word varchar, count int, month_day varchar);";
            System.out.println(sql);
            statement = connection.prepareStatement(sql);
            statement.executeUpdate();
            PreparedStatement preparedStatement = connection.prepareStatement("INSERT into " + name + " VALUES (?,?,?)");
            preparedStatement.setString(3, monthDay);
            String word;
            for (String s : dictionary.keySet()) {
                word = new String(s.getBytes(), "UTF-8");
                preparedStatement.setString(1, word);
                preparedStatement.setInt(2, dictionary.get(s));
                int a = preparedStatement.executeUpdate();
                if (a == 0) {
                    throw new IllegalStateException("Database is unavaliable");
                }
            }
        } catch (SQLException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    private static void processData() {
        Collection<String[]> values = base.values();
        Set<String> strings = new HashSet<String>();
        for (String[] s : values) {
            for (String s1 : s) {
                strings.add(s1);
            }
        }
        for (String s : strings) {
            dictionary.put(s, 0);
        }

        for (String[] s : values) {
            for (String word : s) {
                dictionary.put(word, dictionary.get(word) + 1);
            }
        }
        for (String s : dictionary.keySet()) {
            System.out.println(s + " " + dictionary.get(s));
        }


    }
}
