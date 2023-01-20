package iust.lab.utils;

import iust.lab.model.Point;
import org.joda.time.DateTime;

import java.io.BufferedReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CsvReader {
    private final Pattern pattern;

    public CsvReader(final String separator) {
        this.pattern = Pattern.compile(separator);
    }

    public List<Point> loadCsvContentToList(
            final BufferedReader bufferedReader) throws IOException {
        try {
            return bufferedReader.lines().skip(1).map(line -> {
                final String[] lineArray = pattern.split(line);
                if (Stream.of(lineArray).anyMatch(String::isEmpty)) {
                    return null;
                }
                lineArray[1] = lineArray[1].split(":")[0].length() < 2 ? "0" + lineArray[1] : lineArray[1];
                lineArray[2] = lineArray[2].split("/")[0].length() < 2 ? "0" + lineArray[2] : lineArray[2];
                return Point
                        .builder()
                        .id(lineArray[0])
                        .dateTime(LocalDateTime.of(LocalDate.parse(lineArray[2],
                                DateTimeFormatter.ofPattern("MM/dd/yyyy")),
                                LocalTime.parse(lineArray[1])))
                        .altitude(Integer.parseInt(lineArray[3]))
                        .speed(Integer.parseInt(lineArray[4]))
                        .lat(Float.parseFloat(lineArray[6]))
                        .lon(Float.parseFloat(lineArray[7]))
                        .build();
            })
                    .filter(Objects::nonNull)
                    .sorted(Comparator.comparing(Point::getDateTime, LocalDateTime::compareTo))
                    .collect(Collectors.toList());
        } finally {
            bufferedReader.close();
        }
    }
}
