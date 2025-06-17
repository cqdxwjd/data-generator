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
        // 定义小事务的大小，可根据实际情况调整
        int transactionSize = 1000;
        Random random = new Random();
        for (int transactionStart = start; transactionStart < contractCount; transactionStart += transactionSize) {
            int transactionEnd = Math.min(transactionStart + transactionSize, contractCount);
            executeTransaction(transactionStart, transactionEnd, dayStrs, customerCount, salesStaffCount, batchSize, random);
        }
    }

    private static void executeTransaction(int start, int end, List<String> dayStrs, int customerCount, int salesStaffCount, int batchSize, Random random) {
        Connection con = MySQLUtils.getConnection();
        if (con == null) {
            LOGGER.warn("无法获取数据库连接，退出数据生成，范围: {} - {}", start, end);
            return;
        }

        PreparedStatement pst = null;
        String sql = "insert into contract (id, contract_price, state, sign_day, sales_staff_id, customer_id) values(?, ?, ?, ?, ?, ?)";

        try {
            con.setAutoCommit(false);
            pst = con.prepareStatement(sql);

            for (int i = start; i < end; i++) {
                int dayIndex = random.nextInt(dayStrs.size());
                String dayStr = dayStrs.get(dayIndex);

                int stateIndex = random.nextInt(STATES.size());
                String state = STATES.get(stateIndex);

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
//                    LOGGER.info("已批量插入 {} 条记录，范围: {} - {}", batchSize, start, end);
                }
            }

            // 执行剩余的批量插入
            pst.executeBatch();
            con.commit();
//            LOGGER.info("最终批量插入 {} 条记录，保存到数据库成功，范围: {} - {}", end - start, start, end);
        } catch (Exception e) {
            try {
                if (con != null) {
                    con.rollback();
                }
            } catch (Exception rollbackEx) {
                LOGGER.error("回滚事务失败，范围: {} - {}", start, end, rollbackEx);
            }
            LOGGER.error("保存到数据库失败，范围: {} - {}", start, end, e);
        } finally {
            MySQLUtils.close(con, pst, null);
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
                String day_str = rs.getString("day_str");
                dayStrs.add(day_str);
            }
        } catch (Exception e) {
            LOGGER.error("查询失败", e);
        } finally {
            MySQLUtils.close(con, pst, rs);
        }
        return dayStrs;
    }

    public static void main(String[] args) {
        //合同数
        int contractCount = Config.getIntValue("contractCount") == -1 ? 20000 : Config.getIntValue("contractCount");
        List<String> dayStrs = getDayStrs();
        int customerCount = MySQLUtils.getCount("customer");
        int salesStaffCount = MySQLUtils.getCount("sales_staff");
        int batchSize = Config.getIntValue("batchSize") == -1 ? 1000 : Config.getIntValue("batchSize");
        ContractGenerator.generate(10000, contractCount, dayStrs, customerCount, salesStaffCount, batchSize);
    }
}
