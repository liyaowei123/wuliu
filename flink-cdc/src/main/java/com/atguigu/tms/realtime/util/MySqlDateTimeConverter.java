package com.atguigu.tms.realtime.util;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * @Description:实现CustomConverter接口，重写对应方法对mysql的时间类型进行标准转换
 * @author: WuBo
 * @date:2022/10/11 11:50
 */
public class MySqlDateTimeConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private DateTimeFormatter dateFormatter = DateTimeFormatter.ISO_DATE;

    private DateTimeFormatter timeFormatter = DateTimeFormatter.ISO_TIME;

    private DateTimeFormatter datetimeFormatter = DateTimeFormatter.ISO_DATE_TIME;

    private DateTimeFormatter timestampFormatter = DateTimeFormatter.ISO_DATE_TIME;

    private ZoneId timestampZoneId = ZoneId.systemDefault();

    @Override
    public void configure(Properties props) {

    }

    @Override
    public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {

        String sqlType = column.typeName().toUpperCase();

        SchemaBuilder schemaBuilder = null;

        Converter converter = null;

        if ("DATE".equals(sqlType)) {

            schemaBuilder = SchemaBuilder.string().optional().name("com.darcytech.debezium.date.string");

            converter = this::convertDate;

        }

        if ("TIME".equals(sqlType)) {

            schemaBuilder = SchemaBuilder.string().optional().name("com.darcytech.debezium.time.string");

            converter = this::convertTime;

        }

        if ("DATETIME".equals(sqlType)) {

            schemaBuilder = SchemaBuilder.string().optional().name("com.darcytech.debezium.datetime.string");

            converter = this::convertDateTime;


        }

        if ("TIMESTAMP".equals(sqlType)) {

            schemaBuilder = SchemaBuilder.string().optional().name("com.darcytech.debezium.timestamp.string");

            converter = this::convertTimestamp;

        }

        if (schemaBuilder != null) {

            registration.register(schemaBuilder, converter);

        }

    }


    private String convertDate(Object input) {

        if (input == null) return null;

        if (input instanceof LocalDate) {

            return dateFormatter.format((LocalDate) input);

        }

        if (input instanceof Integer) {

            LocalDate date = LocalDate.ofEpochDay((Integer) input);

            return dateFormatter.format(date);

        }

        return String.valueOf(input);

    }


    private String convertTime(Object input) {

        if (input == null) return null;

        if (input instanceof Duration) {

            Duration duration = (Duration) input;

            long seconds = duration.getSeconds();

            int nano = duration.getNano();

            LocalTime time = LocalTime.ofSecondOfDay(seconds).withNano(nano);

            return timeFormatter.format(time);

        }

        return String.valueOf(input);

    }


    private String convertDateTime(Object input) {

        if (input == null) return null;

        if (input instanceof LocalDateTime) {

            return datetimeFormatter.format((LocalDateTime) input).replaceAll("T", " ");

        }

        return String.valueOf(input);

    }


    private String convertTimestamp(Object input) {

        if (input == null) return null;

        if (input instanceof ZonedDateTime) {

            // mysql的timestamp会转成UTC存储，这里的zonedDatetime都是UTC时间

            ZonedDateTime zonedDateTime = (ZonedDateTime) input;

            LocalDateTime localDateTime = zonedDateTime.withZoneSameInstant(timestampZoneId).toLocalDateTime();

            return timestampFormatter.format(localDateTime).replaceAll("T", " ");

        }
        return String.valueOf(input);
    }
}


