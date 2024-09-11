package com.example.testspark.foreachwriter;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmployeeChecker extends ForeachWriter<Row> {

    private static final long serialVersionUID = 3451238543457743334L;

    private static final Logger log = LoggerFactory.getLogger(EmployeeChecker.class);
    private final String threadName;

    public EmployeeChecker(String threadName) {
        this.threadName = threadName;
    }

    @Override
    public void process(Row value) {
        String jobTitle = value.getString(5).toLowerCase();
        log.debug("{}", jobTitle);
        if (jobTitle.equals("ml")) {
            String firstName = value.getString(0);
            String lastName = value.getString(1);
            long tel = value.getLong(3);
            log.info("Thread {}. {} {} machine learning developer. Tel: {}", threadName, lastName, firstName, tel);
        }
    }

    @Override
    public boolean open(long partitionId, long epochId) {
        return true;
    }

    @Override
    public void close(Throwable errorOrNull) {
    }
}
