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
        int threadCount = 3000;
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
        private final Set<Integer> itemUsed = new HashSet<>();

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
            try (Connection con = MySQLUtils.getConnection()) {
                if (con == null) {
                    return;
                }
                con.setAutoCommit(false);
                String sql = "insert into contract_detail (item_id, item_quantity, detail_price, contract_id, sign_day) values(?, ?, ?, ?, ?);";
                try (PreparedStatement pst = con.prepareStatement(sql)) {
                    for (int j = start; j < end; j++) {
                        itemUsed.clear();
                        float totalPrice = 0;
                        int len = random.nextInt(contractDetailLimit) + 1;
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
                            pst.setInt(4, j);
                            pst.setString(5, dayStr);
                            pst.addBatch();

                            if ((i + 1) % batchSize == 0) {
                                try {
                                    pst.executeBatch();
                                    // 手动清空批处理命令（部分驱动可能需要）
                                    pst.clearBatch();
                                } catch (Exception e) {
                                    try {
                                        con.rollback();
                                        pst.clearBatch(); // 异常时也清空批处理命令
                                    } catch (Exception rollbackEx) {
                                        LOGGER.error("线程 {} 回滚失败，范围: {} - {}", Thread.currentThread().getName(), start, end - 1, rollbackEx);
                                    }
                                    LOGGER.error("线程 {} 执行批处理失败，范围: {} - {}", Thread.currentThread().getName(), start, end - 1, e);
                                }
                            }
                        }
                        updateContractPrice(con, j, totalPrice);
                    }
                    pst.executeBatch();
                    con.commit();
                    LOGGER.info("线程 {} 插入完成，范围: {} - {}", Thread.currentThread().getName(), start, end - 1);
                } catch (Exception e) {
                    try {
                        con.rollback();
                    } catch (Exception rollbackEx) {
                        LOGGER.error("线程 {} 回滚失败，范围: {} - {}", Thread.currentThread().getName(), start, end - 1, rollbackEx);
                    }
                    LOGGER.error("线程 {} 插入失败，范围: {} - {}", Thread.currentThread().getName(), start, end - 1, e);
                }
            } catch (Exception e) {
                LOGGER.error("获取数据库连接失败", e);
            }
        }
    }

    private static void updateContractPrice(Connection con, int contractId, float totalPrice) {
        String sql = "update contract set contract_price = ? where id = ?";
        try (PreparedStatement pst = con.prepareStatement(sql)) {
            pst.setFloat(1, totalPrice);
            pst.setInt(2, contractId);
            pst.executeUpdate();
        } catch (Exception e) {
            LOGGER.error("更新合同总价失败，合同ID: {}", contractId, e);
        }
    }
}