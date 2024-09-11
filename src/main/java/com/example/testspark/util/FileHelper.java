package com.example.testspark.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

public class FileHelper {

    private static final Logger logger = LoggerFactory.getLogger(FileHelper.class);

    private static final String DIRECTORY_PATH1 = "./temp/temp_streaming/";
    private static final String DIRECTORY_PATH2 = "./temp/temp_streaming2/";

    /**
     * Создать директории либо очистить директории от файлов
     * @param directories директории которые требуется создать либо очистить
     */
    public static void createOrCleanTempStreamingDirectory(List<String> directories) {
        for (String patch : directories) {
            createOrCleanTempStreamingDirectory(patch);
        }
    }

    /**
     * Создать директории либо очистить директорию от файлов
     * @param directory директория которую требуется создать либо очистить
     */
    public static void createOrCleanTempStreamingDirectory(String directory) {
        if (tempStreamingDirectoryIsExist(directory)) {
            logger.debug("Clean directory");
            cleanTempStreamingDirectory(directory);
        } else {
            logger.debug("Create directory");
            File temp = new File(directory);
            temp.mkdir();
        }
    }

    /**
     * Метод удаляет все файлы из временной директории
     * @param directory директория из которой нужно удалить все временные файлы
     */
    public static void cleanTempStreamingDirectory(String directory) {
        if (!tempStreamingDirectoryIsExist(directory)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(Path.of(directory))) {
            paths.filter(Files::isRegularFile).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    logger.error("An exception occurred while trying to delete a file: {}, exception: {}", p, e.getMessage());
                }
            });
        } catch (IOException e) {
            logger.error("An exception occurred while creating a stream: {}", e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Метод возвращает true елси директория для временных файлов существует и false если не существует
     * @param directory путь до директории формат строка
     * @return true елси директория существует и false если не существует
     */
    public static boolean tempStreamingDirectoryIsExist(String directory) {
        return Files.exists(Path.of(directory));
    }

    /**
     * Метод возвращает путь до директории с файлами
     * @return возвращает строку содержащую путь от директории для временных файлов
     */
    public static String getTempStreamingDirectoryPath() {
        return DIRECTORY_PATH1;
    }

    /**
     * Метод возвращает путь до директории с файлами
     * @return возвращает строку содержащую путь от директории для временных файлов
     */
    public static String getTempStreamingDirectoryPath2() {
        return DIRECTORY_PATH2;
    }
}
