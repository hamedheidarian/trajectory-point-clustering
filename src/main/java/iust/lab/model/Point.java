package iust.lab.model;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

@Data
@Setter
@Builder
public class Point {
    private String id;
    private long dateTime;
    private int altitude;
    private int speed;
    private float lat;
    private float lon;
}
