package org.eu.mmacedo.mysql.log.sink.domain;

import java.time.LocalDateTime;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@Table("log")
public class LogEntry {
    private @Id byte[] uuid;
    private LocalDateTime date;
    private Integer ip;
    private String method;
    private Integer response;
    private String agent;    
}
