package org.apdplat.data.generator.generator;

import org.apdplat.data.generator.mysql.MySQLUtils;
import org.apdplat.data.generator.utils.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by ysc on 18/04/2018.
 */
public class ContractGenerator {
    private static final Logger LOGGER = LoggerFactory.getLogger(ContractGenerator.class);

    private static final List<String> STATES = Arrays.asList("新建", "签订", "生效", "履行中", "终止", "作废");

    public static void clear() {
        MySQLUtils.clean("contract");
    }

    public static void generate(int contractCount, List<String> dayStrs, int customerCount, int salesStaffCount, int batchSize) {
        generate(0, contractCount, dayStrs, customerCount, salesStaffCount, batchSize);
    }

    public static void generate(int start, int contractCount, List<String> dayStrs, int customerCount, int salesStaffCount, int batchSize) {
        // 获取可用处理器核心数，作为线程池大小
//        int threadCount = Runtime.getRuntime().availableProcessors();
        int threadCount = 64;
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);

        int chunkSize = (contractCount - start) / threadCount;
        for (int i = 0; i < threadCount; i++) {
            int threadStart = start + i * chunkSize;
            int threadEnd = (i == threadCount - 1) ? contractCount : threadStart + chunkSize;
            executorService.submit(new InsertTask(threadStart, threadEnd, dayStrs, customerCount, salesStaffCount, batchSize));
        }

        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error("线程池等待中断", e);
        }
        LOGGER.info("所有线程插入完成");
    }

    private static class InsertTask implements Runnable {
        private final int start;
        private final int end;
        private final List<String> dayStrs;
        private final int customerCount;
        private final int salesStaffCount;
        private final int batchSize;
        private final Random random = new Random(System.nanoTime());

        InsertTask(int start, int end, List<String> dayStrs, int customerCount, int salesStaffCount, int batchSize) {
            this.start = start;
            this.end = end;
            this.dayStrs = dayStrs;
            this.customerCount = customerCount;
            this.salesStaffCount = salesStaffCount;
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
            String sql = "insert into contract (id, contract_price, state, sign_day, sales_staff_id, customer_id) values(?, ?, ?, ?, ?, ?);";
            try {
                con.setAutoCommit(false);
                pst = con.prepareStatement(sql);
                for (int i = start; i < end; i++) {
                    int r = random.nextInt(dayStrs.size());
                    String dayStr = dayStrs.get(r);
                    r = random.nextInt(STATES.size());
                    String state = STATES.get(r);
                    int salesStaffId = random.nextInt(salesStaffCount) + 1;
                    int customerId = random.nextInt(customerCount) + 1;
                    int contractId = i + 1;
                    pst.setInt(1, contractId);
                    pst.setFloat(2, 0);
                    pst.setString(3, state);
                    pst.setString(4, dayStr);
                    pst.setInt(5, salesStaffId);
                    pst.setInt(6, customerId);
                    pst.addBatch();

                    if ((i + 1) % batchSize == 0) {
                        pst.executeBatch();
                    }
                }
                pst.executeBatch();
                con.commit();
                LOGGER.info("线程 {} 插入完成，范围: {} - {}", Thread.currentThread().getName(), start, end - 1);
            } catch (Exception e) {
                try {
                    if (con != null) {
                        con.rollback();
                    }
                } catch (Exception rollbackEx) {
                    LOGGER.error("线程 {} 回滚失败，范围: {} - {}", Thread.currentThread().getName(), start, end - 1, rollbackEx);
                }
                LOGGER.error("线程 {} 插入失败，范围: {} - {}", Thread.currentThread().getName(), start, end - 1, e);
            } finally {
                MySQLUtils.close(con, pst, rs);
            }
        }
    }

    private static List<String> getDayStrs() {
        List<String> dayStrs = new ArrayList<>();
        Connection con = MySQLUtils.getConnection();
        if (con == null) {
            return dayStrs;
        }
        PreparedStatement pst = null;
        ResultSet rs = null;
        try {
            String sql = "select day_str from day_dimension";
            LOGGER.info("开始查询, SQL: {}", sql);
            pst = con.prepareStatement(sql);
            rs = pst.executeQuery();
            LOGGER.info("查询结束, 开始处理数据");
            while (rs.next()) {
                String dayStr = rs.getString("day_str");
                dayStrs.add(dayStr);
            }
        } catch (Exception e) {
            LOGGER.error("查询失败", e);
        } finally {
            MySQLUtils.close(con, pst, rs);
        }
        return dayStrs;
    }

    public static void main(String[] args) {
        // 合同数
        int contractCount = Config.getIntValue("contractCount") == -1 ? 20000 : Config.getIntValue("contractCount");
        List<String> dayStrs = getDayStrs();
        int customerCount = MySQLUtils.getCount("customer");
        int salesStaffCount = MySQLUtils.getCount("sales_staff");
        int batchSize = Config.getIntValue("batchSize") == -1 ? 1000 : Config.getIntValue("batchSize");
        ContractGenerator.generate(10000, contractCount, dayStrs, customerCount, salesStaffCount, batchSize);
    }
}