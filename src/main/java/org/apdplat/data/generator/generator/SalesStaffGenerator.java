package org.apdplat.data.generator.generator;

import org.apdplat.data.generator.mysql.MySQLUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by ysc on 18/04/2018.
 */
public class SalesStaffGenerator {
    private static final Logger LOGGER = LoggerFactory.getLogger(SalesStaffGenerator.class);

    public static void clear() {
        MySQLUtils.clean("sales_staff");
    }

    public static List<String> generate(int areaCount, int customerCount, int batchSize) {
        return generate(areaCount, customerCount, batchSize, null);
    }

    public static List<String> generate(int areaCount, int salesStaffCount, int batchSize, Collection<String> exclude) {
        List<String> names = PeopleNames.getNames(salesStaffCount);
        int threadCount = 2;
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);

        int chunkSize = salesStaffCount / threadCount;
        for (int i = 0; i < threadCount; i++) {
            int start = i * chunkSize;
            int end = (i == threadCount - 1) ? salesStaffCount : start + chunkSize;
            executorService.submit(new InsertTask(areaCount, names, start, end, batchSize));
        }

        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error("线程池等待中断", e);
        }

        return names;
    }

    static class InsertTask implements Runnable {
        private final int areaCount;
        private final List<String> names;
        private final int start;
        private final int end;
        private final int batchSize;

        InsertTask(int areaCount, List<String> names, int start, int end, int batchSize) {
            this.areaCount = areaCount;
            this.names = names;
            this.start = start;
            this.end = end;
            this.batchSize = batchSize;
        }

        @Override
        public void run() {
            Connection con = MySQLUtils.getConnection();
            if (con == null) {
                return;
            }
            PreparedStatement pst = null;
            ResultSet rs = null;
            String sql = "insert into sales_staff (name, gender, area_id) values(?, ?, ?);";
            try {
                con.setAutoCommit(false);
                pst = con.prepareStatement(sql);
                Random random = new Random(System.nanoTime());
                for (int i = start; i < end; i++) {
                    pst.setString(1, names.get(i));
                    pst.setString(2, random.nextInt(names.size()) > names.size() / 2 ? "男" : "女");
                    pst.setInt(3, random.nextInt(areaCount) + 1);
                    pst.addBatch();

                    if ((i + 1 - start) % batchSize == 0) {
                        pst.executeBatch();
                    }
                }
                pst.executeBatch();
                con.commit();
                LOGGER.info("线程 {} 保存到数据库成功", Thread.currentThread().getName());
            } catch (Exception e) {
                LOGGER.error("线程 {} 保存到数据库失败", Thread.currentThread().getName(), e);
            } finally {
                MySQLUtils.close(con, pst, rs);
            }
        }
    }

    public static void main(String[] args) {
        clear();
        generate(30, 3, 1000);
    }
}