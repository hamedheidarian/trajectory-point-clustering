package iust.lab.model;

import lombok.*;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class Point implements Serializable {
    private Integer id;
    private Long dateTime;
    private Integer altitude;
    private Integer speed;
    private Float lat;
    private Float lon;
}
