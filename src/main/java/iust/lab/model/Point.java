package iust.lab.model;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

@Getter
@Setter
@Builder
public class Point {
    private String id;
    private LocalDateTime dateTime;
    private int altitude;
    private int speed;
    //    private int heading;
    private float lat;
    private float lon;
//    private float age;
//    private float range;
//    private float bearing;
//    private String tail;
//    private String metar;
}
