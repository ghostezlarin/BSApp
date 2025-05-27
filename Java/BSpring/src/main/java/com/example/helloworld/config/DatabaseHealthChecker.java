package com.example.helloworld.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class DatabaseHealthChecker implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseHealthChecker.class);
    private final JdbcTemplate jdbcTemplate;

    public DatabaseHealthChecker(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void run(String... args) {
        try {
            String dbVersion = jdbcTemplate.queryForObject("SELECT version()", String.class);
            logger.info("Database connection successful. Version: {}", dbVersion);
        } catch (Exception e) {
            logger.error("DATABASE CONNECTION FAILED!", e);
            throw new RuntimeException("Database connection failed", e);
        }
    }
}