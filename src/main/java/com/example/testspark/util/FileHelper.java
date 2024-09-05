package com.example.testspark.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class FileHelper {

    private static final Logger logger = LoggerFactory.getLogger(FileHelper.class);

    private static final String DIRECTORY_PATH = "./temp_streaming/";

    /**
     * Создать директории либо очистить директорию от файлов
     */
    public static void createOrCleanTempStreamingDirectory() {
        if (tempStreamingDirectoryIsExist()) {
            logger.debug("Clean directory");
            cleanTempStreamingDirectory();
        } else {
            logger.debug("Create directory");
            File temp = new File(DIRECTORY_PATH);
            temp.mkdir();
        }
    }

    /**
     * Метод удаляет все файлы из временной директории
     */
    public static void cleanTempStreamingDirectory() {
        if (!tempStreamingDirectoryIsExist()) {
            return;
        }
        try (Stream<Path> paths = Files.walk(Path.of(DIRECTORY_PATH))) {
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
     * @return true елси директория существует и false если не существует
     */
    public static boolean tempStreamingDirectoryIsExist() {
        return Files.exists(Path.of(DIRECTORY_PATH));
    }

    /**
     * Метод возвращает путь до директории с файлами
     * @return возвращает строку содержащую путь от директории для временных файлов
     */
    public static String getTempStreamingDirectoryPath() {
        return DIRECTORY_PATH;
    }
}
