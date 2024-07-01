package com.example.testspark.config;

import com.example.testspark.dao.impl.ExampleDAOImpl;
import com.example.testspark.dao.interfaces.ExampleDAO;

public class Init {

    private Init(){}

    /**
     * Метод выполняет проверку что таблица example_table существует
     * Если таблица существует то exampleDAO.getExampleTableIsExist() вернет 1
     * Если вернет 0 то создаем схему и таблице
     */
    public static void execute() {
        ExampleDAO exampleDAO = new ExampleDAOImpl();
        try {
            if (exampleDAO.getExampleTableIsExist() > 0) {
                return;
            }
            exampleDAO.createExampleSchema();
            exampleDAO.createExampleTable();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
