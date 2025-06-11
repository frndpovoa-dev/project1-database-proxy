package dev.frndpovoa.project1.databaseproxy.service;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.hash.Hashing;
import lombok.NonNull;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.util.UUID;

import static java.time.temporal.ChronoField.*;

@Service
public class UniqueIdGenerator {
    private final DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendValue(YEAR, 4, 4, SignStyle.EXCEEDS_PAD)
            .appendValue(MONTH_OF_YEAR, 2)
            .appendValue(DAY_OF_MONTH, 2)
            .appendValue(HOUR_OF_DAY, 2)
            .appendValue(MINUTE_OF_HOUR, 2)
            .appendValue(SECOND_OF_MINUTE, 2)
            .appendFraction(NANO_OF_SECOND, 9, 9, false)
            .toFormatter();
    private final LoadingCache<String, String> hashingCache = CacheBuilder.newBuilder()
            .build(new CacheLoader<>() {
                @Override
                public String load(String groupName) {
                    return Hashing.sha256()
                            .hashString(groupName, StandardCharsets.UTF_8).toString()
                            .replaceAll("^(.{7}).*", "$1");
                }
            });

    public String generate(@NonNull Class<?> clazz) {
        return UUID.randomUUID().toString().replaceAll("-", "")
                + dateTimeFormatter.format(OffsetDateTime.now())
                + hashingCache.getUnchecked(clazz.getName());
    }
}
