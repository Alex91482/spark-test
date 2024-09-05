package com.example.testspark.generators;

import com.example.testspark.dao.entity.StreamModel;
import com.example.testspark.util.FileHelper;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreadFileGenerator {

    private static final Logger logger = LoggerFactory.getLogger(ThreadFileGenerator.class);

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private final int defaultTimeSecondExecute = 60;

    private final List<String> titles = Arrays.asList("Admin", "Manager", "Backend Developer", "Architect", "Frontend Developer", "DevOps", "ML");
    private final List<String> firstNames = Arrays.asList("Liam", "Noah", "William", "James", "Emma", "Olivia", "Ava", "Isabella");
    private final List<String> lastNames = Arrays.asList("Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson");

    public void execute() {
        CompletableFuture.runAsync(() -> {
            long start = System.currentTimeMillis();
            var executeTime = start + defaultTimeSecondExecute * 1000L;
            job(executeTime);
        });
    }

    public void execute(final int timeSecondExecute) {
        CompletableFuture.runAsync(() -> {
            try {
                var start = System.currentTimeMillis();
                var executeTime = start + timeSecondExecute * 1000L;
                job(executeTime);

            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private void job(long executeTime) {
        var directory = FileHelper.getTempStreamingDirectoryPath();
        while (executeTime > System.currentTimeMillis()) {
            var fileName = directory + UUID.randomUUID() + ".json";
            var list = generateData();
            var mapper = new ObjectMapper();
            try {
                mapper.writeValue(new File(fileName), list);
                Thread.sleep(randomNumberFromOneToFive() * 500L);
            } catch (IOException e) {
                logger.error("An exception occurred while writing the file: {}", e.getMessage());
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Метод генерирует данные в количестве 10 штук
     * @return список из 10 строк данных
     */
    private List<StreamModel> generateData() {
        var list = new ArrayList<StreamModel>();
        for(int i = 0; i < 10; i++) {
            var streamModel = new StreamModel();
            streamModel.setFirstName(randomFirstName());
            streamModel.setLastName(randomLastName());
            streamModel.setBirthDate(randomLocalDate());
            streamModel.setUuid(String.valueOf(UUID.randomUUID()));
            streamModel.setPhoneNumber(randomPhoneNumber());
            streamModel.setJobTitle(randomTitle());
            list.add(streamModel);
        }
        return list;
    }

    /**
     * Метод создает рандомную датц рождения от 1950 до 1999 года
     * @return возвращает дату рождения
     */
    private LocalDate randomLocalDate() {
        int year = 1900 + (50 + (int) (Math.random() * 50));
        int month = 1 + (int) (Math.random() * 12);
        int day = 1 + (int) (Math.random() * 28);
        return LocalDate.of(year, month, day);
    }

    /**
     * Метод выбирает рандомное число от 1 до 5
     * @return возвращает число от 1 до 5
     */
    private int randomNumberFromOneToFive() {
        int minValue = 1;
        int maxValue = 5;
        return minValue + (int) (Math.random() * (maxValue - minValue + 1));
    }

    /**
     * Метод создает случайный мобильный номер
     * @return возвращает номер мобильного телефона
     */
    private long randomPhoneNumber() {
        long minValue = 0L;
        long maxValue = 999999999L;
        return 89000000000L + (minValue + (long) (Math.random() * (maxValue - minValue + 1)));
    }

    /**
     * Метод выбирает из списка в случайном порядке должность
     * @return возвращает должность
     */
    private String randomTitle() {
        int titleNumber = (int) (Math.random() * (titles.size()));
        return titles.get(titleNumber);
    }

    /**
     * Метод выбирает из списка в случайном порядке имя
     * @return возвращает имя
     */
    private String randomFirstName() {
        int firstNameNumber = (int) (Math.random() * (firstNames.size()));
        return firstNames.get(firstNameNumber);
    }

    /**
     * Метод выбирает из списка в случайном порядке фамилию
     * @return возвращает фамилию
     */
    private String randomLastName() {
        int lastNameNumber = (int) (Math.random() * (lastNames.size()));
        return lastNames.get(lastNameNumber);
    }

    /*public static void main(String...args) {
        ThreadFileGenerator threadFileGenerator = new ThreadFileGenerator();
        threadFileGenerator.execute(30);
        try {
            Thread.sleep(30000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }*/
}
