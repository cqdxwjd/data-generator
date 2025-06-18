package org.apdplat.data.generator.generator;

import org.apdplat.data.generator.mysql.MySQLUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by ysc on 18/04/2018.
 */
public class ContractDetailGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ContractDetailGenerator.class);

    public static void clear() {
        MySQLUtils.clean("contract_detail");
    }

    public static void generate(int contractCount, int contractDetailLimit, int itemQuantityLimit, Map<Integer, Float> items, List<String> dayStrs, int batchSize) {
        // 设置为数据库连接池数量的1倍以上
        int threadCount = 32;
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);

        int chunkSize = contractCount / threadCount;
        for (int i = 0; i < threadCount; i++) {
            int start = i * chunkSize + 1;
            int end = (i == threadCount - 1) ? contractCount + 1 : start + chunkSize;
            executorService.submit(new InsertTask(start, end, contractDetailLimit, itemQuantityLimit, items, dayStrs, batchSize));
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
        private final int contractDetailLimit;
        private final int itemQuantityLimit;
        private final Map<Integer, Float> items;
        private final List<String> dayStrs;
        private final int batchSize;
        private final Random random = new Random(System.nanoTime());

        InsertTask(int start, int end, int contractDetailLimit, int itemQuantityLimit, Map<Integer, Float> items, List<String> dayStrs, int batchSize) {
            this.start = start;
            this.end = end;
            this.contractDetailLimit = contractDetailLimit;
            this.itemQuantityLimit = itemQuantityLimit;
            this.items = items;
            this.dayStrs = dayStrs;
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
            try {
                con.setAutoCommit(false);
                String sql = "insert into contract_detail (item_id, item_quantity, detail_price, contract_id, sign_day) values(?, ?, ?, ?, ?);";
                pst = con.prepareStatement(sql);
                for (int j = start; j < end; j++) {
                    int contractId = j;
                    float totalPrice = 0;
                    int len = random.nextInt(contractDetailLimit) + 1;
                    Set<Integer> itemUsed = new HashSet<>();
                    for (int i = 0; i < len; i++) {
                        int r = random.nextInt(dayStrs.size());
                        String dayStr = dayStrs.get(r);
                        int itemId = random.nextInt(items.size()) + 1;
                        while (itemUsed.contains(itemId)) {
                            itemId = random.nextInt(items.size()) + 1;
                        }
                        itemUsed.add(itemId);
                        int itemQuantity = random.nextInt(itemQuantityLimit) + 1;
                        float detailPrice = items.get(itemId) * itemQuantity;
                        totalPrice += detailPrice;
                        pst.setInt(1, itemId);
                        pst.setInt(2, itemQuantity);
                        pst.setFloat(3, detailPrice);
                        pst.setInt(4, contractId);
                        pst.setString(5, dayStr);
                        pst.addBatch();

                        if ((i + 1) % batchSize == 0) {
                            pst.executeBatch();
                        }
                    }
                    updateContractPrice(con, contractId, totalPrice);
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

    private static void updateContractPrice(Connection con, int contractId, float totalPrice) {
        try {
            con.prepareStatement("update contract set contract_price=" + totalPrice + " where id=" + contractId).executeUpdate();
        } catch (Exception e) {
            LOGGER.error("更新合同总价失败，合同ID: {}", contractId, e);
        }
    }
}