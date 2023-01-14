package iust.lab.utils;

import iust.lab.model.Point;

import java.io.BufferedReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class CsvReader {
    private final Pattern pattern;

    public CsvReader(final String separator) {
        this.pattern = Pattern.compile(separator);
    }

    public List<Point> loadCsvContentToList(
            final BufferedReader bufferedReader) throws IOException {
        try  {
            return bufferedReader.lines().skip(1).map(line -> {
                final String[] lineArray = pattern.split(line);
                return Point
                        .builder()
                        .id(lineArray[0])
                        .time(LocalTime.parse(lineArray[1]))
                        .date(LocalDate.parse(lineArray[2]))
                        .altitude(Integer.parseInt(lineArray[3]))
                        .speed(Integer.parseInt(lineArray[4]))
                        .lat(Float.parseFloat(lineArray[6]))
                        .lon(Float.parseFloat(lineArray[7]))
                        .build();
            }).collect(Collectors.toList());
        } finally {
            bufferedReader.close();
        }
    }
}
