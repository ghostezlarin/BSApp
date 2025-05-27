package com.example.helloworld.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

@Service
public class OrganizationService {
    private static final Logger logger = LoggerFactory.getLogger(OrganizationService.class);
    private final JdbcTemplate jdbcTemplate;
    private static final String ORGANIZATION_SCHEMA = "PPI"; // Константа схемы организации

    public OrganizationService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public String getLatestDebtByAccount(String accountNumber) {
        try {
            logger.info("Используемая схема: {}", ORGANIZATION_SCHEMA);
            // 1. Находим последнюю таблицу в схеме
            String latestTable = findLatestTable();
            if (latestTable == null) {
                return "Не найдено подходящих таблиц в схеме " + ORGANIZATION_SCHEMA;
            }

            logger.info("Используем таблицу: {}", latestTable);

            // 2. Получаем структуру таблицы для проверки
            List<String> columns = getTableColumns(latestTable);
            if (!columns.contains("debt") || !columns.contains("account_number")) {
                return "Таблица " + latestTable + " не содержит нужных колонок";
            }

            // 3. Выполняем запрос к последней таблице
            String sql = String.format(
                    "SELECT debt FROM \"%s\".\"%s\" WHERE account_number = ?",
                    ORGANIZATION_SCHEMA,
                    latestTable
            );

            logger.info("Выполняем запрос: {}", sql);
            BigDecimal debt = jdbcTemplate.queryForObject(sql, BigDecimal.class, accountNumber);

            return String.format("Задолженность по счету %s: %s", accountNumber, debt);

        } catch (EmptyResultDataAccessException e) {
            return "Счет " + accountNumber + " не найден";
        } catch (Exception e) {
            logger.error("Ошибка при выполнении запроса", e);
            return "Ошибка сервера: " + e.getMessage();
        }
    }

    private String findLatestTable() {
        // Ищем все таблицы в схеме, которые соответствуют шаблону t_PPI_число
        logger.debug("Поиск таблицы в схеме: {}", ORGANIZATION_SCHEMA);
        String sql = "SELECT table_name FROM information_schema.tables " +
                "WHERE table_schema = ? AND table_name ~ '^t_" + ORGANIZATION_SCHEMA + "_\\d+$' " +
                "ORDER BY substring(table_name from '_(\\d+)$')::integer DESC " +
                "LIMIT 1";

        try {
            return jdbcTemplate.queryForObject(sql, String.class, ORGANIZATION_SCHEMA);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    private List<String> getTableColumns(String tableName) {
        String sql = "SELECT column_name FROM information_schema.columns " +
                "WHERE table_schema = ? AND table_name = ?";
        return jdbcTemplate.queryForList(sql, String.class, ORGANIZATION_SCHEMA, tableName);
    }
}